import os
import pandas as pd
import numpy as np #Import numpy to handle NaN values
import camelot
import re
import PyPDF2
import matplotlib.pyplot as plt
import traceback
import logging
import subprocess
from mysecrets import PDF_DIR, CSV_FILE

# Turn on for logging during testing!
print_all = 'off'
print_extract = 'on'
print_page = 'off'
print_plot = 'off'
print_logs = 'off'
if print_all == 'on':
    print_extract = 'on'
    print_page = 'on'
    print_plot = 'on'
    print_logs = 'on'

def get_pdf_files_recursive(PDF_DIR): # Function to get a list of PDF files in a directory and its subdirectories
    pdf_files = []
    for root, _, files in os.walk(PDF_DIR):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    return pdf_files

headers_to_include = ["Date", "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"] # Global variable to define headers to include

def pypdf2_extract_text_from_pdf(pdf_path): # Uses PyPDF2 to extract information that Camelot misses (namely account #)
    try:
        with open(pdf_path, 'rb') as pdf_file:
            pdf_reader = PyPDF2.PdfFileReader(pdf_file)

            full_text = ""
            for page_number in range(pdf_reader.numPages):
                page = pdf_reader.getPage(page_number)
                full_text += page.extract_text()

            return full_text
    except ValueError as ve:
        print("PyPDF2 Error: ", ve)
    except IndexError as ie:
        print("PyPDF2 Error: ", ie)
    except Exception as e:
        print("An unexpected PyPDF2 error occurred", e)

# Function to extract tables from the PDF using Camelot in stream mode
def extract_tables_with_camelot(pdf_path): 
    # Use PyPDF2 to extract the text from the first page
    pypdf2_text_extract = pypdf2_extract_text_from_pdf(pdf_path)
    if print_extract == 'on':
        print("------------------PyPDF2-----------------")
        print(pypdf2_text_extract)
        print("----------------PyPDF2 End---------------")

    # Search for the pattern "Your RBC personal <anything> account statement"
    match = re.search(r"Your\s+RBC\s+personal\s+.*?\s*account\s+statement", pypdf2_text_extract, re.IGNORECASE)
    if not match:
        return None

    if print_all == 'on':
        print("Match: " + str(match))

    # Find the string right below the matched pattern
    year_pattern = r"(\d{4})"
    year_match = re.search(year_pattern, pypdf2_text_extract[match.end():])
    if not year_match:
        year = "2000"
    else:
        year = year_match.group(1)  # Extract the captured group containing the year
    if print_all == 'on':
        print("Year: " + year)

    # Find the account number after "Your account number:"
    account_number = re.search(r'Your account number:\s*(\d{5}-\d{7})', pypdf2_text_extract)
    account_number = account_number.group(1) if account_number else None
    if print_all == 'on':
        print("Account number: " + account_number)
    
    # Extract data from pages with camelot-py and combines
    tables_pages = []
    tables_page1 = camelot.read_pdf(pdf_path, flavor='stream', pages='1', edge_tol=46, column_tol=0, row_tol=0, suppress_stdout=True) # Extract pg 1 with explicit parameters
    tables_page2_plus = camelot.read_pdf(pdf_path, flavor='stream', pages='2-end', edge_tol=30, column_tol=2, row_tol=2, suppress_stdout=True) # Extract remaining pgs with different parameters
    
    # Set pandas display to show all columns
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    for table in tables_page1:
        tables_pages.append(table)

    for table in tables_page2_plus:
        tables_pages.append(table)

    # Remove tables that don't conform to the expected transaction table
    threshold_columns = 5
    filtered_tables_pages = [table for table in tables_pages if table.shape[1] >= threshold_columns]

    if print_all == 'on': # Print the parsing report
        print("")
        print("Parsing Report")
        print (tables_pages[0].parsing_report)
        print("")
    if print_extract == 'on': # Loop through each table and print its content
        print("-----------------Camelot-----------------")
        for table in filtered_tables_pages:
            print(table.df)
        print("---------------Camelot End---------------")
        print("")
    if print_plot == 'on': # Show PDF table plot
        #camelot.plot(tables_pages[0], kind='text')
        #camelot.plot(tables_pages[0], kind='grid')
        #camelot.plot(tables_pages[0], kind='textedge')
        camelot.plot(filtered_tables_pages[1], kind='textedge')
        try:
            camelot.plot(filtered_tables_pages[2], kind='textedge')
        except Exception as e:
            pass
        plt.show(block=True)

    dataframes = []

    for table in filtered_tables_pages:
        if table.df.empty:
            continue

        # Find the index of the header row
        header_index = table.df[table.df.apply(lambda row: all(header in " ".join(row) for header in headers_to_include), axis=1)].index
        if print_all == 'on':
            print("Header index: " + str(header_index))

        if len(header_index) > 0:
            table.df = table.df.iloc[header_index[0]:] # If the header row is found, set the DataFrame to rows starting from that index

            try: # Fix for PDF extracts that concat Date & Description columns
                string_to_find = "Date\nDescription" # Sets the name to find (based on the desired column name) from the contents
                is_match = table.df.iloc[0].isin([string_to_find]) # Check if the first row contains the specified string
                if any(is_match):
                    col_index = int(is_match[is_match].index[0]) # Get the column index where the match is True
                    table.df.insert(col_index, 'Date', table.df[col_index]) # Duplicate the column to the left by inserting it at the same index
                    table.df.loc[~table.df['Date'].str.contains(r'\n'), 'Date'] = "" # In the date column, replace any values that don't have "\n" in them with " "
                    table.df['Date'] = table.df['Date'].str.replace(r'\n.*', '', regex=True) # In the date column, trim "\n"
                
                    # In the description column, trim anything from the "\" of the first "\n"
                    is_match = table.df.iloc[0].isin([string_to_find]) # Check if the first row contains the specified string
                    col_index = int(is_match[is_match].index[0]) # Get the column index where the match is True
                    table.df[col_index] = table.df[col_index].str.replace(r'.*\n', '', regex=True)

                    # Update column headers to match standard header pattern
                    #table.df.rename(columns={1: 0, 2: 1, 3: 2, 4: 3}, inplace=True)
                
                    if print_all == 'on':
                        print("---------")
                        print("Fix for concatenated Date & Description columns:")
                        print(table.df)

            except ValueError:
                # Handle the case where the column name is not found
                print(r"Column 'Date\nDescription' not found")

        else:
            continue # If the header row is not found, skip this table

        # If Date isn't the first column, shift all columns left, replace NaNs with blank strings (Removes the vertical left column string for page 1)
        if ("Date" not in table.df.iloc[:, 0].values) and ("Date" in table.df.iloc[:, 1].values) and (table.page == 1):
            table.df = table.df.shift(periods=-1, axis=1)
            table.df.rename(columns={0: 'Date', 'Date': 0}, inplace=True)
            if print_all == 'on':
                print("Table after Date shift:")
                print(table.df)

            table.df.fillna('', inplace=True)

            # Drop the last column (index 5) from the DataFrame
            table.df = table.df.drop(table.df.columns[5], axis=1)
            if print_all == 'on':
                print("Table after NaN fill and last column (5) drop:")
                print(table.df)

        # Find the index of "Opening Balance" to remove it
        opening_balance_index = table.df[table.df.apply(lambda row: "Opening Balance" in " ".join(row), axis=1)].index
        if len(opening_balance_index) > 0:
            table.df = table.df.drop(opening_balance_index[0]) # If "Opening Balance" is found, set the DataFrame to rows until that index.

        # Find the index of "Closing Balance" to remove rows from it and after
        end_index = table.df.loc[table.df.apply(lambda row: "Closing Balance" in " ".join(row), axis=1)].index
        if not end_index.empty:
            table.df = table.df.loc[:end_index.values[0] - 1]

        if (table.page) != 1:
            # Standardize page 2+ column headers to match standardized headers
            table.df.rename(columns={0: 'Date', 1: 0, 2: 1, 3: 2, 4: 3}, inplace=True)

        #Append the year to the "Date" column for non-empty rows
        table.df["Date"] = [f"{date}, {year}" if (date.strip() and date != "Date") else date for date in table.df["Date"]]

        # Adds the account number to the table
        table.df.insert(1, "Account Number", "") # Insert new column for Account Numbers
        table.df["Account Number"] = [f"{account_number}" if description.strip() else description for description in table.df[0]] # Append the account number to the "Account Number" column for non-empty description rows

        # Fixes multiline concatenation - loops through the DataFrame starting from the second row
        for i in range(1, len(table.df)):
            if table.df.iloc[i, 2].strip(): # Check if the description is not empty for the current row
                if table.df.iloc[i, 3] == '' and table.df.iloc[i, 4] == '': # Check if both withdrawals and deposits are empty for the current row
                    table.df.iloc[i+1, 2] = table.df.iloc[i, 2] + ' | ' + table.df.iloc[i+1, 2] # Concatenate the description with the next row's description
                    table.df.iloc[i, 2] = '' # Clear out the current row's description

        table.df = table.df[table.df.iloc[:, 2].str.strip() != ''] # Drop rows where the description is empty

        dataframes.append(table.df) # Append the DataFrame to the list
        
        if print_page == 'on': # Loop through each table and print its content
            print("")
            print(f"---------Page {table.page} processed data:----------")
            print(table.df)
            print(f"-------Page {table.page} processed data End:--------")

    return dataframes

# Function to process PDFs and save data to CSV
def process_pdfs(): # Retrieves the files
    pdf_files = get_pdf_files_recursive(PDF_DIR)
    total_files = len(pdf_files)
    processed_files = 0
    progress_mark = 0
    all_dataframes = []
    not_processed = []

    # Logging related items
    not_processed_log_path = os.path.join(os.path.dirname(PDF_DIR), r'!not_processed.txt') # Construct the log file path based on the PDF file path
    error_log_path = os.path.join(os.path.dirname(PDF_DIR), r'!error_log.txt') # Construct the log file path based on the PDF file path
    with open(error_log_path, 'w'):
        pass
    logging.basicConfig(filename=error_log_path, level=logging.ERROR, format='%(asctime)s - %(message)s') # Set up logging to write errors to the log file in the same directory as the PDF file

    for pdf_path in pdf_files: # Proceses all files in the given directory
        try:
            dataframes_camelot = extract_tables_with_camelot(pdf_path)
            if dataframes_camelot:
                if print_logs == 'on':
                    print(f"Processed {pdf_path}")
                all_dataframes.extend(dataframes_camelot)
            else:
                if print_logs == 'on':
                    print(f"Didn't process {pdf_path}")
                not_processed.append(pdf_path)
                continue
            processed_files += 1 # These lines handle log messages for % file completion
            percentage = (processed_files/total_files) * 100
            if percentage >= progress_mark + 4: # Print the update message for every x% completion
                progress_mark = (int(percentage) // 4) * 4
                print(f"{progress_mark:.2f}% of files processed.")
        except Exception as e:
            logging.error("Didn't process: %s", pdf_path)
            if print_logs == 'on':
                print(f"An error occurred processing the file: {pdf_path} |", e)
            #traceback.print_exc()

    if all_dataframes: # Data extract post-processing cleanup
        combined_data = pd.concat(all_dataframes, ignore_index=True) # Concatenate all dataframes into a single DataFrame
        #pd.set_option('display.max_rows', None)

        
        if pd.isna(combined_data.iloc[0, -1]): # Drop the last column of NaN values
            combined_data = combined_data.drop(combined_data.columns[-1], axis=1)
            #combined_data = combined_data.drop(combined_data.index[-1])

        combined_data = combined_data.replace('', np.nan) # Replace empty strings with NaN
        #combined_data = combined_data.dropna(how='all') # Drop rows that are completely empty in all columns
        
        # Remove rows containing headers
        headers_to_exclude = ["Date", re.escape(".*"), "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"]
        for header in headers_to_exclude:
            combined_data = combined_data[~combined_data.apply(lambda row: header in " ".join(row.astype(str)), axis=1)]

        # Set the DataFrame columns using the headers from the first page
        #combined_data.columns = headers_to_include

        combined_data.columns = ["Date", "Account #", "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"]

        # Forward-fill missing dates in the "Date" column
        combined_data["Date"].fillna(method='ffill', inplace=True)

        csv_path = os.path.join(PDF_DIR, f"{CSV_FILE}.csv")
        combined_data.to_csv(csv_path, index=False)
        if print_logs == 'on':
            print(f"Data saved to {csv_path}")

        if not_processed:
            with open(not_processed_log_path, 'w') as file:
                file.write("\n".join(not_processed))
            if print_logs == 'on':
                print(f"Unprocessed log file saved to {not_processed_log_path}")

        if print_page == 'on': # Loop through each table and print its content
            print("-------------Combined data:--------------")
            print(combined_data)
            print("----------End of combined data:----------")
    else:
        if print_logs == 'on':
            print("No data extracted from any PDFs.")

    try:
        # Redirect stdout and stderr to a log file
        log_file_path = 'script_log.txt'
        with open(log_file_path, 'w') as log_file:
            # Run the main script and capture the terminal output in the log file
            subprocess.call(['python', 'pdf2csv.py'], stdout=log_file, stderr=subprocess.STDOUT)

        # Move the log file to the PDF directory
        if os.path.exists(log_file_path):
            log_file_dest = os.path.join(PDF_DIR, '!script_log.txt')
            os.rename(log_file_path, log_file_dest)
            print(f"Log file saved to: {log_file_dest}")
        else:
            print("Log file not found.")
    except Exception as e:
        if print_logs == 'on':
            print("A script log file error occurred")

# Replace 'YOUR_PDF_DIRECTORY' and 'output_csv_file' with your desired values in the mysecrets.py file.
try:
    process_pdfs()
except Exception as e:
    if print_logs == 'on':
        print("A PDF processing error has occurred: ", e)
    traceback.print_exc()