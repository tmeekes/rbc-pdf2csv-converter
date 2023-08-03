import os
import pandas as pd
import numpy as np #Import numpy to handle NaN values
import camelot
import re
import PyPDF2
import matplotlib.pyplot as plt
from mysecrets import PDF_DIR, CSV_FILE

# Function to get a list of PDF files in a directory and its subdirectories
def get_pdf_files_recursive(PDF_DIR):
    pdf_files = []
    for root, _, files in os.walk(PDF_DIR):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    return pdf_files

# Global variable to define headers to include
headers_to_include = ["Date", "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"]

def pypdf2_extract_text_from_pdf(pdf_path, page_number):
    with open(pdf_path, 'rb') as pdf_file:
        pdf_reader = PyPDF2.PdfFileReader(pdf_file)
        if page_number < 1 or page_number > pdf_reader.numPages:
            raise ValueError(f"Invalid page number. Must be between 1 and {pdf_reader.numPages}.")

        page = pdf_reader.getPage(page_number - 1)  # Page numbers are 0-based
        return page.extract_text()

# Function to extract tables from the PDF using Camelot in stream mode
def extract_tables_with_camelot(pdf_path):
    
    # Use PyPDF2 to extract the text from the first page
    pypdf2_text_extract = pypdf2_extract_text_from_pdf(pdf_path, page_number=1)

    # Search for the pattern "Your RBC personal <anything> account statement"
    match = re.search(r"Your\s+RBC\s+personal\s+.*\s+account\s+statement", pypdf2_text_extract, re.IGNORECASE)
    # if not match:
    #     return None

    # Find the string right below the matched pattern
    year_pattern = r"(\d{4})"
    year_match = re.search(year_pattern, pypdf2_text_extract[match.end():])
    # if not year_match:
    #     return None

    year = year_match.group(1)  # Extract the captured group containing the year

    # Find the account number after "Your account number:"
    account_number = re.search(r'Your account number:\s*(\d{5}-\d{7})', pypdf2_text_extract)
    account_number = account_number.group(1) if account_number else None
    
    # Read the first page separately, all others grouped in one loop afterwards
    tables_page1 = camelot.read_pdf(pdf_path, flavor='stream', pages='1', edge_tol=34)
    tables_page2_onwards = camelot.read_pdf(pdf_path, flavor='stream', pages='2-end', edge_tol=22)
    
    dataframes = []

    for table in tables_page1:
        if table.df.empty:
            continue

        # Find the index of the header row
        header_index = table.df[table.df.apply(lambda row: all(header in " ".join(row) for header in headers_to_include), axis=1)].index

        if len(header_index) > 0:
            # If the header row is found, set the DataFrame to rows starting from that index
            table.df = table.df.iloc[header_index[0]:]
        else:
            # If the header row is not found, skip this table
            continue

        # If Date isn't the first column, shift all columns left, replace NaNs with blank strings (Removes the vertical left column string)
        if ("Date" not in table.df.iloc[:, 0].values) and ("Date" in table.df.iloc[:, 1].values):
            table.df = table.df.shift(periods=-1, axis=1)

            table.df.fillna('', inplace=True)

            # Drop the last column (index 5) from the DataFrame
            table.df = table.df.drop(table.df.columns[5], axis=1)

        # Find the index of "Opening Balance" to remove it
        opening_balance_index = table.df[table.df.apply(lambda row: "Opening Balance" in " ".join(row), axis=1)].index
        if len(opening_balance_index) > 0:
            # If "Opening Balance" is found, set the DataFrame to rows until that index.
            #table.df = table.df.iloc[opening_balance_index[0]]
            table.df = table.df.drop(opening_balance_index[0])

        # Find the index of "Closing Balance" to remove rows from it and after
        end_index = table.df.loc[table.df.apply(lambda row: "Closing Balance" in " ".join(row), axis=1)].index
        if not end_index.empty:
            table.df = table.df.loc[:end_index.values[0] - 1]

        #Append the year to the "Date" column for non-empty rows
        #table.df[0] = [f"{date}, {year}" if date.strip() else date for date in table.df[0]]
        table.df[0] = [f"{date}, {year}" if (date.strip() and date != "Date") else date for date in table.df[0]]
        #table.df[0] = [f"{date}, {year}" if date.strip() and date != "Date" else date for date in table.df[0]]

        # Insert new column for Account Numbers
        table.df.insert(1, "Account Number", "")

        # Append the account number to the "Account Number" column for non-empty rows
        table.df["Account Number"] = [f"{account_number}" if description.strip() else description for description in table.df[1]]

        # Loop through the DataFrame starting from the second row
        for i in range(1, len(table.df)):
            # Check if the description is not empty for the current row
            if table.df.iloc[i, 2].strip():
                # Check if both withdrawals and deposits are empty for the current row
                if table.df.iloc[i, 3] == '' and table.df.iloc[i, 4] == '':
                    # Concatenate the description with the next row's description
                    table.df.iloc[i+1, 2] = table.df.iloc[i, 2] + ' | ' + table.df.iloc[i+1, 2]
                    # Clear out the current row's description
                    table.df.iloc[i, 2] = ''

        # Drop rows where the description is empty
        table.df = table.df[table.df.iloc[:, 2].str.strip() != '']

        # Append the DataFrame to the list
        dataframes.append(table.df)

    for table in tables_page2_onwards:
        if table.df.empty:
            continue

        # Find the index of the header row
        header_index = table.df[table.df.apply(lambda row: all(header in " ".join(row) for header in headers_to_include), axis=1)].index

        if len(header_index) > 0:
            # If the header row is found, set the DataFrame to rows starting from the row after that index
            table.df = table.df.iloc[header_index[0] + 1:]
        else:
            # If the header row is not found, skip this table
            continue

        # Find the index of "Opening Balance" to remove it
        opening_balance_index = table.df[table.df.apply(lambda row: "Opening Balance" in " ".join(row), axis=1)].index
        if len(opening_balance_index) > 0:
            # If "Opening Balance" is found, set the DataFrame to rows until that index. *** For some reason, it's using a row well below "Closing Balance"... manually adjusted, but need to revisit to clean up
            #table.df = table.df.iloc[opening_balance_index[0]]
            table.df = table.df.drop(opening_balance_index[0])

        # Find the index of "Closing Balance" to remove rows from it and after
        end_index = table.df.loc[table.df.apply(lambda row: "Closing Balance" in " ".join(row), axis=1)].index
        if not end_index.empty:
            table.df = table.df.loc[:end_index.values[0] - 1]

        #Append the year to the "Date" column for non-empty rows
        table.df[0] = [f"{date}, {year}" if date.strip() else date for date in table.df[0]]

        # Insert new column for Account Numbers
        table.df.insert(1, "Account Number", "")

        # Append the account number to the "Account Number" column for non-empty rows
        table.df["Account Number"] = [f"{account_number}" if description.strip() else description for description in table.df[1]]

        # Loop through the DataFrame starting from the second row
        for i in range(1, len(table.df)):
            # Check if the description is not empty for the current row
            if table.df.iloc[i, 2].strip():
                # Check if both withdrawals and deposits are empty for the current row
                if table.df.iloc[i, 3] == '' and table.df.iloc[i, 4] == '':
                    # Concatenate the description with the next row's description
                    table.df.iloc[i+1, 2] = table.df.iloc[i, 2] + ' | ' + table.df.iloc[i+1, 2]
                    # Clear out the current row's description
                    table.df.iloc[i, 2] = ''

        # Drop rows where the description is empty
        table.df = table.df[table.df.iloc[:, 2].str.strip() != '']

        # Append the DataFrame to the list
        dataframes.append(table.df)

    return dataframes

# Function to process PDFs and save data to CSV
def process_pdfs():
    pdf_files = get_pdf_files_recursive(PDF_DIR)
    all_dataframes = []

    for pdf_path in pdf_files:
        dataframes_camelot = extract_tables_with_camelot(pdf_path)
        if dataframes_camelot:
            print(f"Processed {pdf_path}")
            all_dataframes.extend(dataframes_camelot)
        else:
            print(f"Didn't process {pdf_path}")
            continue

    if all_dataframes:
        # Concatenate all dataframes into a single DataFrame
        combined_data = pd.concat(all_dataframes, ignore_index=True)
        #pd.set_option('display.max_rows', None)

        # Drop the last column of NaN values
        if pd.isna(combined_data.iloc[0, -1]):
            combined_data = combined_data.drop(combined_data.columns[-1], axis=1)
            #combined_data = combined_data.drop(combined_data.index[-1])

        # Replace empty strings with NaN
        combined_data = combined_data.replace('', np.nan)
        # Drop rows that are completely empty in all columns
        #combined_data = combined_data.dropna(how='all')
        
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
        print(combined_data)
        print(f"Data saved to {csv_path}")
    else:
        print("No data extracted from any PDFs.")

# Replace 'YOUR_PDF_DIRECTORY' and 'output_csv_file' with your desired values in the mysecrets.py file.
process_pdfs()