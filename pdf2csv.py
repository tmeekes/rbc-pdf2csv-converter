import os
import pandas as pd
import numpy as np #Import numpy to handle NaN values
import camelot
import re
import PyPDF2
import warnings
import matplotlib.pyplot as plt
import traceback
import logging
import subprocess
from tqdm import tqdm
from mysecrets import PDF_DIR, CSV_FILE

# Turn on for logging during testing!
print_all = 'off'
print_extract = 'off'
print_page = 'off'
print_plot = 'off'
print_logs = 'off'
print_errors = 'off'
print_trace = 'on'
#print_progress = 'off'

if print_all == 'on':
    print_extract = 'on'
    print_page = 'on'
    print_plot = 'on'
    print_logs = 'on'
    print_errors = 'on'
    print_trace = 'on'
    #print_progress = 'on'
if print_errors == 'off':
    warnings.filterwarnings("ignore") # Suppress PDFReadWarnings

headers_to_include = ["Date", "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"] # Global variable to define headers to include
credit_headers_to_include = ["TRANSACTION", "ACTIVITY DESCRIPTION", "AMOUNT ($)"] # Global variable to define the credit headers to include

def get_pdf_files_recursive(PDF_DIR): # Function to get a list of PDF files in a directory and its subdirectories
    pdf_files = []
    for root, _, files in os.walk(PDF_DIR):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    return pdf_files

def pypdf2_extract_from_pdf(pdf_path): # Uses PyPDF2 to extract initial information from account statements (account #, year)
    with open(pdf_path, 'rb') as pdf_file:
        pdf_reader = PyPDF2.PdfFileReader(pdf_file)
        pypdf2_full_extract = ""
        statement_type = ""
        for page_number in range(pdf_reader.numPages):
            page = pdf_reader.getPage(page_number)
            pypdf2_full_extract += page.extract_text()

        if print_extract == 'on':
            print("------------------PyPDF2-----------------")
            print(pypdf2_full_extract)
            print("----------------PyPDF2 End---------------")

        # Search for the pattern "Your RBC personal <anything> account statement"
        match = re.search(r"Your\s+RBC\s+personal\s+.*?\s*account\s+statement", pypdf2_full_extract, re.IGNORECASE)
        if match:
            statement_type = "account"
            if print_logs == 'on':
                print("Found " + statement_type + " match")
            # Find the account number after "Your account number:"
            account_number = re.search(r'Your account number:\s*(\d{5}-\d{7})', pypdf2_full_extract)
            account_number = account_number.group(1) if account_number else None
        if not match:
            match = re.search(r".*RBC.*(?:Visa|Mastercard).*", pypdf2_full_extract, re.IGNORECASE)
            if match:
                statement_type = "credit"
                if print_logs == 'on':
                    print("Found " + statement_type + " match")
                # Find the credit card number
                account_number = re.search(r'(?:\b\d{4}\s\d{2}\*\*\s\*\*\*\*\s\d{4}\b)', pypdf2_full_extract, re.IGNORECASE)
                account_number = account_number.group(0) if account_number else None
            if not match:
                return None

        if print_all == 'logs':
            print("Account number: " + account_number)

        # Find the string right below the matched pattern
        year_pattern = r"(20\d{2})"
        year_matches = re.findall(year_pattern, pypdf2_full_extract[match.end():])
        year2 = ""
        if not year_matches:
            year = "2000"
        else:
            if len(year_matches) >= 2 and year_matches[0] != year_matches[1]:
                year = year_matches[0]
                year2 = year_matches[1]
            else:
                year = year_matches[0]
                year2 = year
        
        if print_logs == 'on':
            print("Account: " + str(account_number))
            print("Year: " + year)
            print("Year2: " + year2)

    return year, year2, account_number, statement_type

def extract_account_tables_with_camelot(pdf_path, year, year2, account_number): # Function to extract tables from the PDF using Camelot in stream mode and perform initial processing
    
    # Set pandas display to show all columns
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    # Extract data from pages with camelot-py and combines
    tables_pgs = []
    
    tables_pg1 = camelot.read_pdf(pdf_path, flavor='stream', pages='1', edge_tol=32, column_tol=2, row_tol=2, suppress_stdout=True) # Extract pg 1 with explicit parameters
    cleaned_pg1 = [table for table in tables_pg1 if table.shape[1] >= 5]
    if len(cleaned_pg1) == 0:
        tables_pg1 = camelot.read_pdf(pdf_path, flavor='stream', pages='1', edge_tol=50, suppress_stdout=True) # When the right tables aren't found, adjust the parameters to try a better tolerance
        cleaned_pg1 = [table for table in tables_pg1 if table.shape[1] >= 5]
        if len(cleaned_pg1) == 0:
            tables_pg1 = camelot.read_pdf(pdf_path, flavor='stream', pages='1', edge_tol=100, suppress_stdout=True) # When the 5 column table isn't found, adjust for a 4 column table
            cleaned_pg1 = [table for table in tables_pg1 if table.shape[1] >= 4]

    tables_pg2p = camelot.read_pdf(pdf_path, flavor='stream', pages='2-end', edge_tol=32, column_tol=2, row_tol=2, suppress_stdout=True) # Extract all pages after 1 with alternate parameters
    cleaned_pg2p = [table for table in tables_pg2p if table.shape[1] >= 5]
    if len(cleaned_pg2p) == 0:
        tables_pg2p = camelot.read_pdf(pdf_path, flavor='stream', pages='2-end', edge_tol=22, column_tol=2, row_tol=2, suppress_stdout=True) # When the right tables aren't found, adjust the parameters to try a better tolerance
        cleaned_pg2p = [table for table in tables_pg2p if table.shape[1] >= 5]

    # Combines tables series
    tables_pgs.extend(cleaned_pg1)
    tables_pgs.extend(cleaned_pg2p)

    if print_all == 'on': # Print the parsing report
        print("")
        print("Parsing Report")
        if len(tables_pgs) == 0:
            print("No parsing report available")
        else:
            print (tables_pgs[0].parsing_report)
        print("")
    if print_extract == 'on': # Loop through each table and print its content
        print("-----------------Camelot-----------------")
        for table in tables_pgs:
            print(table.df)
        print("---------------Camelot End---------------")
        print("")
    if print_plot == 'on': # Show PDF table plot
        try:
            camelot.plot(tables_pgs[0], kind='textedge')
        except Exception as e:
            pass
        try:
            camelot.plot(tables_pgs[1], kind='textedge')
        except Exception as e:
            pass
        plt.show(block=True)

    dataframes = []

    for table in tables_pgs:
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
                    if print_all == 'on':
                        print("---------")
                        print("Fix for concatenated Date & Description columns:")
                        print(table.df)
            except ValueError: # Handle the case where the column name is not found
                print(r"Column 'Date\nDescription' not found")
        else:
            continue # If the header row is not found, skip this table

        # Identify the columns that contain any of the required headers
        selected_columns = []
        for col in table.df.columns:
            if any(string in table.df[col].values for string in headers_to_include):
                new_col_name = f"{table.df[col].iloc[0]}"
                if 'Withdrawals ($)' in new_col_name: # Look for withdrawals in the new column name
                    new_col_name = 'Credit ($)'
                if 'Deposits ($)' in new_col_name: # Look for description in the new column name
                    new_col_name = 'Debit ($)'
                table.df.rename(columns={col: new_col_name}, inplace=True) # rename the columns
                selected_columns.append(new_col_name)
        table.df = table.df[selected_columns] # Create a new DataFrame with only the desired columns

        # Find the index of "Opening Balance" to remove it
        opening_balance_index = table.df[table.df.apply(lambda row: "Opening Balance" in " ".join(row), axis=1)].index
        if len(opening_balance_index) > 0:
            table.df = table.df.drop(opening_balance_index[0]) # If "Opening Balance" is found, set the DataFrame to rows until that index.

        # Find the index of "Closing Balance" to remove rows from it and after
        end_index = table.df.loc[table.df.apply(lambda row: "Closing Balance" in " ".join(row), axis=1)].index
        if not end_index.empty:
            table.df = table.df.loc[:end_index.values[0] - 1]

        #Append the year to the "Date" column for non-empty rows
        if year == year2:
            table.df["Date"] = [f"{date}, {year}" if (date.strip() and date != "Date") else date for date in table.df["Date"]]
        else:
            table.df["Date"] = [f"{date}, {year2}" if "Jan" in date and (date.strip() and date != "Date") else f"{date}, {year}" if (date.strip() and date != "Date") else date for date in table.df["Date"]]

        # Adds the account number to the table
        table.df.insert(1, "Account #", "") # Insert new column for Account Numbers

        # Fixes multiline concatenation - loops through the DataFrame starting from the second row
        for i in range(1, len(table.df)):
            if table.df.iloc[i, 2].strip(): # Check if the description is not empty for the current row
                table.df.iloc[i, 1] = account_number
                if table.df.iloc[i, 3] == '' and table.df.iloc[i, 4] == '': # Check if both withdrawals and deposits are empty for the current row
                    table.df.iloc[i+1, 2] = table.df.iloc[i, 2] + ' | ' + table.df.iloc[i+1, 2] # Concatenate the description with the next row's description
                    table.df.iloc[i, 2] = '' # Clear out the current row's description
                    table.df.iloc[i+1, 0] = table.df.iloc[i, 0] + table.df.iloc[i+1, 0] # Concatenate the date with the next row's date
        table.df = table.df[table.df.iloc[:, 2].str.strip() != ''] # Drop rows where the description is empty OR indicates no activity
        table.df = table.df[table.df.iloc[:, 2].str.strip() != 'No activity for this period'] # Drop rows where the description is empty OR indicates no activity

        # for col in table.df.columns: # Renames the columns for consistency with credit statements
        #     if table.df[col].apply(lambda x: any(substring in str(x) for substring in headers_to_include)).any():
        #         new_col_name = f"{table.df[col].iloc[0]}"
        #         if 'Withdrawals ($)' in new_col_name: # Look for withdrawals in the new column name
        #             new_col_name = 'Credit ($)'
        #         if 'Deposits ($)' in new_col_name: # Look for description in the new column name
        #             new_col_name = 'Debit ($)'
        #         table.df.rename(columns={col: new_col_name}, inplace=True) # rename the columns
        
        dataframes.append(table.df) # Append the DataFrame to the list

        if print_page == 'on': # Loop through each table and print its content
            print("")
            print(f"---------Page {table.page} processed data:----------")
            print(table.df)
            print(f"-------Page {table.page} processed data End:--------")
        
    return dataframes

def extract_credit_tables_with_camelot(pdf_path, year, year2, account_number):
    
    # Set pandas display to show all columns
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)

    # Extract data from pages with camelot-py and combines
    tables_pgs = []
    #table_areas = ['50, 720, 360, 50']
    table_areas = ['50, 595, 360, 50']
    column_bounds = ['95.8, 125, 306']
    
    tables_pg1 = camelot.read_pdf(pdf_path, flavor='stream', table_areas=table_areas, columns=column_bounds, pages='1', edge_tol=1, column_tol=0, row_tol=10, suppress_stdout=True) # Extract pg 1 with explicit parameters
    #tables_pg1 = camelot.read_pdf(pdf_path, flavor='stream', columns=column_bounds, pages='1', edge_tol=1, column_tol=1, row_tol=10, suppress_stdout=True) # Extract pg 1 with explicit parameters
    cleaned_pg1 = [table for table in tables_pg1 if table.shape[1] >= 3]
    # if len(cleaned_pg1) == 0:
    #     tables_pg1 = camelot.read_pdf(pdf_path, flavor='stream', pages='1', edge_tol=50, suppress_stdout=True) # When the right tables aren't found, adjust the parameters to try a better tolerance
    #     cleaned_pg1 = [table for table in tables_pg1 if table.shape[1] >= 1]
    #     if len(cleaned_pg1) == 0:
    #         tables_pg1 = camelot.read_pdf(pdf_path, flavor='stream', pages='1', edge_tol=100, suppress_stdout=True) # When the 5 column table isn't found, adjust for a 4 column table
    #         cleaned_pg1 = [table for table in tables_pg1 if table.shape[1] >= 1]

    tables_pg2p = camelot.read_pdf(pdf_path, flavor='stream', pages='2-end', edge_tol=1, column_tol=0, row_tol=10, suppress_stdout=True) # Extract all pages after 1 with alternate parameters
    cleaned_pg2p = [table for table in tables_pg2p if table.shape[1] >= 3]
    # if len(cleaned_pg2p) == 0:
    #     tables_pg2p = camelot.read_pdf(pdf_path, flavor='stream', pages='2-end', edge_tol=22, column_tol=2, row_tol=2, suppress_stdout=True) # When the right tables aren't found, adjust the parameters to try a better tolerance
    #     cleaned_pg2p = [table for table in tables_pg2p if table.shape[1] >= 1]

    # Combines tables series
    tables_pgs.extend(cleaned_pg1)
    tables_pgs.extend(cleaned_pg2p)

    if print_all == 'on': # Print the parsing report
        print("")
        print("Parsing Report")
        if len(tables_pgs) == 0:
            print("No parsing report available")
        else:
            print (tables_pgs[0].parsing_report)
        print("")
    if print_extract == 'on': # Loop through each table and print its content
        print("-----------------Camelot-----------------")
        for table in tables_pgs:
            print(table.df)
        print("---------------Camelot End---------------")
        print("")
    if print_plot == 'on': # Show PDF table plot
        try:
            camelot.plot(tables_pgs[0], kind='textedge')
        except Exception as e:
            pass
        try:
            camelot.plot(tables_pgs[1], kind='textedge')
        except Exception as e:
            pass
        plt.show(block=True)

    dataframes = []

    for table in tables_pgs:
        if table.df.empty:
            continue

        # Find the index of the header row
        header_index = table.df[table.df.apply(lambda row: row.str.contains("activity description", case=False).any(), axis=1)].index
        if print_all == 'on':
            print("Header index: " + str(header_index))
        if len(header_index) > 0:
            table.df = table.df.iloc[header_index[0]:] # If the header row is found, set the DataFrame to rows starting from that index
        else:
            continue # If the header row is not found, skip this table

        # Identify the columns that contain any of the required headers
        selected_columns = []
        for col in table.df.columns:
            if table.df[col].apply(lambda x: any(substring in str(x) for substring in credit_headers_to_include)).any():
                new_col_name = f"{table.df[col].iloc[0]}"
                if 'DATE' in new_col_name: # Look for date in the new column name
                    new_col_name = 'Date'
                if 'DESCRIPTION' in new_col_name: # Look for description in the new column name
                    new_col_name = 'Description'
                if 'AMOUNT ($)' in new_col_name: # Look for description in the new column name
                    new_col_name = 'Credit ($)'
                table.df.rename(columns={col: new_col_name}, inplace=True) # rename the columns
                selected_columns.append(new_col_name)
        table.df = table.df[selected_columns] # Create a new DataFrame with only the desired columns

        # Find the index of "TOTAL ACCOUNT BALANCE" to remove rows from it and after
        end_index_tb = table.df.loc[table.df.apply(lambda row: "TOTAL ACCOUNT BALANCE" in " ".join(row), axis=1)].index
        if not end_index_tb.empty:
            table.df = table.df.loc[:end_index_tb.values[0] - 1]

        # Find the index of "NEW BALANCE" to remove rows from it and after
        end_index_nb = table.df.loc[table.df.apply(lambda row: "NEW BALANCE" in " ".join(row), axis=1)].index
        if not end_index_nb.empty:
            table.df = table.df.loc[:end_index_nb.values[0] - 1]

        #Append the year to the "Date" column for non-empty rows
        if year == year2:
            table.df["Date"] = [f"{date}, {year}" if (date.strip() and date != "Date") else date for date in table.df["Date"]]
        else:
            table.df["Date"] = [f"{date}, {year2}" if "JAN" in date and (date.strip() and date != "Date") else f"{date}, {year}" if (date.strip() and date != "Date") else date for date in table.df["Date"]]

        # Adds the account number to the table
        table.df.insert(1, "Account #", "") # Insert new column for Account Numbers
        table.df.insert(4, "Debit ($)", "") # Insert new debit column for splitting negative values to
        table.df.insert(5, "Balance ($)", "") # Insert a balance column to match the account statements for consistency in processing

        # Loops through the DataFrame starting from the second row to fix multiline concatenation, split out negative values to the debit column, add the account number
        table.df = table.df.reset_index(drop=True)
        for i in range(1, len(table.df)):
            if table.df.loc[i, 'Description'].lower().startswith('foreign currency'):
                table.df.loc[i-1, 'Description'] = table.df.loc[i-1, 'Description'] + ' | ' + table.df.loc[i, 'Description']
                table.df.loc[i, 'Description'] = ''
            if table.df.loc[i, 'Date'].strip() and table.df.loc[i, 'Credit ($)'].strip(): # Check if the date and amount rows are empty for the current row
                table.df.iloc[i, 1] = account_number # Add in the account number
            if table.df.loc[i, 'Credit ($)'].startswith('-'): #Check if the value is negative
                table.df.loc[i, 'Debit ($)'] = table.df.loc[i, 'Credit ($)'][1:] # Copy the value to the debit column and make it positive
                table.df.loc[i, 'Credit ($)'] = '' # Erase the original value
        table.df['Description'] = table.df['Description'].str.replace('\n', ' | ')
        table.df = table.df[table.df.iloc[:, 2].str.strip() != ''] # Drop rows where the description is empty OR indicates no activity
        table.df = table.df[table.df.iloc[:, 2].str.strip() != 'No activity for this period'] # Drop rows where the description is empty OR indicates no activity

        dataframes.append(table.df) # Append the DataFrame to the list

        if print_page == 'on': # Loop through each table and print its content
            print("")
            print(f"---------Page {table.page} processed data:----------")
            print(table.df)
            print(f"-------Page {table.page} processed data End:--------")
        
    return dataframes

def post_extraction_processing(dataframes): # Handles additional formatting of full dataframe series to clean the data once its been standardized and combined
    
    for df in dataframes:
        if df.index.duplicated().any():
            print("Duplicate index values found in a Dataframe")
    
    # Reset the index of each DataFrame
    dataframes = [df.reset_index(drop=True) for df in dataframes]

    data = pd.concat(dataframes, ignore_index=True) # Concatenate all dataframes into a single DataFrame
    #pd.set_option('display.max_rows', None)

    
    if pd.isna(data.iloc[0, -1]): # Drop the last column of NaN values
        data = data.drop(data.columns[-1], axis=1)

    data = data.replace('', np.nan) # Replace empty strings with NaN
    #data = data.dropna(how='all') # Drop rows that are completely empty in all columns
    
    # Remove rows containing headers
    # headers_to_exclude = ["Date", re.escape(".*"), "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"]
    # for header in headers_to_exclude:
    #     data = data[~data.apply(lambda row: header in " ".join(row.astype(str)), axis=1)]

    headers_set1 = ["Date", re.escape(".*"), "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"]
    headers_set2 = [r"TRANSACTION POSTING\nDATE", re.escape(".*"), "ACTIVITY DESCRIPTION", "AMOUNT ($)", re.escape(".*"), re.escape(".*")]
    #headers_set2 = ["TRANSACTION POSTING\nDATE", "ACTIVITY DESCRIPTION", "AMOUNT ($)"]
    for headers_to_exclude in [headers_set1, headers_set2]:
        for header in headers_to_exclude:
            data = data[~data.apply(lambda row: header in " ".join(row.astype(str)), axis=1)]

    data.columns = ["Date", "Account #", "Description", "Credit ($)", "Debit ($)", "Balance ($)"]

    # Forward-fill missing dates in the "Date" column
    data["Date"].fillna(method='ffill', inplace=True)

    return data

def process_pdfs(): # Function to process PDFs and save data to CSV
    pdf_files = get_pdf_files_recursive(PDF_DIR)
    all_dataframes = []
    not_processed = []

    # Logging related items
    not_processed_log_path = PDF_DIR + r'\!pdf2csv_unprocessed.txt' # Construct the log file path based on the PDF file path
    error_log_path = os.path.join(PDF_DIR, r'\!pdf2csv_error_log.txt') # Construct the log file path based on the PDF file path
    with open(error_log_path, 'w'):
        pass
    logging.basicConfig(filename=error_log_path, level=logging.ERROR, format='%(asctime)s - %(message)s') # Set up logging to write errors to the log file in the same directory as the PDF file

    #for pdf_path in pdf_files: # Proceses all files in the given directory
    for pdf_path in tqdm(pdf_files, desc="Processing files", unit="file", leave=False): # Proceses all files in the given directory
        try:
            statement_year, statement_year2, statement_acct_num, statement_type = pypdf2_extract_from_pdf(pdf_path)
        except ValueError as ve:
            if print_errors == 'on': # Prints except errors when enabled
                print("PyPDF2 extract error: ", ve)
        except IndexError as ie:
            if print_errors == 'on': # Prints except errors when enabled
                print("PyPDF2 extract error: ", ie)
        except Exception as e:
            if print_errors == 'on': # Prints except errors when enabled
                print("An unexpected PyPDF2 extract error occurred: ", e)

        try:
            if statement_type == 'account':
                dataframes_camelot = extract_account_tables_with_camelot(pdf_path, statement_year, statement_year2, statement_acct_num)
            elif statement_type == 'credit':
                dataframes_camelot = extract_credit_tables_with_camelot(pdf_path, statement_year, statement_year2, statement_acct_num)
            else:
                print("No statement type")
            if dataframes_camelot:
                if print_logs == 'on':
                    print(f"Processed {pdf_path}")
                all_dataframes.extend(dataframes_camelot)
            else:
                if print_logs == 'on':
                    print(f"Didn't process {pdf_path}")
                not_processed.append(pdf_path)
        except Exception as e:
            logging.error("Didn't process: %s", pdf_path)
            if print_logs == 'on':
                print(f"A camelot error occurred processing the file: {pdf_path} |", e)
            if print_trace == 'on':
                traceback.print_exc()

    if all_dataframes: # Data extract post-processing cleanup
        combined_data = post_extraction_processing(all_dataframes)

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
        log_file = 'script_log.txt'
        with open(log_file, 'w') as log_file:
            # Run the main script and capture the terminal output in the log file
            subprocess.call(['python', 'pdf2csv.py'], stdout=log_file, stderr=subprocess.STDOUT)

        # Move the log file to the PDF directory - not working properly, currently, due to a windows conflict
        if os.path.exists(log_file):
            #log_file_dest = PDF_DIR + r'\!pdf2csv_script_log.txt'
            log_file_dest = r'\!pdf2csv_script_log.txt'
            os.rename(log_file, log_file_dest)
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
    if print_trace == 'on':
        traceback.print_exc()