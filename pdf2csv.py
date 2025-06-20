# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# A copy of the GNU General Public License can be found at
# <https://www.gnu.org/licenses/>.

import os
import pandas as pd
import numpy as np #Import numpy to handle NaN values
import camelot
import re
import PyPDF2
import pypdf
import pdfplumber
import warnings
import matplotlib.pyplot as plt
import traceback
import logging
import subprocess
from tqdm import tqdm
from mysecrets import PDF_DIR, CSV_FILE
from datetime import datetime

# Custom options for testing!
save_file = 'on'
print_all = 'off'
print_extract = 'off'
print_page = 'off'
print_plot = 'off'
print_logs = 'off'
print_errors = 'on'
print_trace = 'on'
print_progress = 'on'


if print_all == 'on':
    print_extract = 'on'
    print_page = 'on'
    print_plot = 'on'
    print_logs = 'on'
    print_errors = 'on'
    print_trace = 'on'
    #print_progress = 'on'
if print_errors == 'on':
    warnings.filterwarnings("ignore") # Suppress PDFReadWarnings

headers = ["Date", "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"] # Define headers to use from table
cc_headers = ["DATE", "ACTIVITY DESCRIPTION", "AMOUNT ($)"] # Credit card headers
cl_headers = ["Date", "Description", "Interest/Fees/Insurance ($)", "Withdrawals ($)", "Payments ($)", "Balance owing ($)"] # Credit line headers

def get_pdf_files_recursive(PDF_DIR): # Function to get a list of PDF files in a directory and its subdirectories
    pdf_files = []
    for root, _, files in os.walk(PDF_DIR):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    return pdf_files

def pypdf_extract_from_pdf(pdf_path): # Uses PyPDF2 to extract initial information from account statements (account #, year)
    statement_type = "unknown"
    with open(pdf_path, 'rb') as pdf_file:
        # pdf_reader = PyPDF2.PdfFileReader(pdf_file)
        pdf_reader = pypdf.PdfReader(pdf_file)
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
                print("\nFound " + statement_type + " match")
            # Find the account number after "Your account number:"
            account_number = re.search(r'Your account number:\s*(\d{5}-\d{7})', pypdf2_full_extract)
            account_number = account_number.group(1) if account_number else None
        if not match:
            match = re.search(r".*RBC.*(?:Visa|Mastercard).*", pypdf2_full_extract, re.IGNORECASE)
            if match:
                statement_type = "credit"
                if print_logs == 'on':
                    print("\nFound " + statement_type + " match")
                # Find the credit card number
                account_number = re.search(r'(?:\b\d{4}\s\d{2}\*\*\s\*\*\*\*\s\d{4}\b)', pypdf2_full_extract, re.IGNORECASE)
                account_number = account_number.group(0) if account_number else None
                print(account_number)
        if not match:
            match = re.search(r"Your\s+Royal\s+Credit\s+Line", pypdf2_full_extract, re.IGNORECASE)
            if match:
                statement_type = "credit_line"
                if print_logs == 'on':
                    print("\nFound " + statement_type + " match")
                # Find the credit card number
                #account_number = re.search(r'(^\d{8}-\d{3}$)', pypdf2_full_extract, re.IGNORECASE) # Searches based on just the account number
                account_number = re.search(r'(Your loan account number:\s*\d{8}-\d{3})', pypdf2_full_extract, re.IGNORECASE) # Searches based on the statement + account number
                account_number = account_number.group(0) if account_number else None
            if not match:
                return None

        if print_all == 'logs':
            print("Account number: " + account_number)

        # Find the string right below the matched pattern
        year_pattern = r", (20\d{2})"
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

def pdfplumber_extract_from_pdf(pdf_path):  # Updated to use pdfplumber
    statement_type = "unknown"
    pdf_extract = ""
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text(x_tolerance=1, y_tolerance=4, layout=True)
            if text:
                pdf_extract += text + "\n"

    if print_extract == 'on':
        print("------------------pdfplumber-----------------")
        print(pdf_extract)
        print("----------------pdfplumber End---------------")

    # Search for the pattern "Your RBC personal <anything> account statement"
    match = re.search(r"Your\s+RBC\s+personal\s+.*?\s*account\s+statement", pdf_extract, re.IGNORECASE)
    if match:
        statement_type = "account"
        if print_logs == 'on':
            print("\nFound " + statement_type + " match")
        # Find the account number after "Your account number:"
        account_number = re.search(r'Your account number:\s*(\d{5}-\d{7})', pdf_extract)
        account_number = account_number.group(1) if account_number else None
    
    if not match:
        match = re.search(r".*RBC.*(?:Visa|Mastercard).*", pdf_extract, re.IGNORECASE)
        if match:
            statement_type = "credit"
            if print_logs == 'on':
                print("\nFound " + statement_type + " match")
            # Find the credit card number
            account_number = re.search(r'(?:\b\d{4}\s\d{2}\*\*\s\*\*\*\*\s\d{4}\b)', pdf_extract, re.IGNORECASE)
            account_number = account_number.group(0) if account_number else None
    
    if not match:
        #match = re.search(r"Your\s+Royal\s+Credit\s+Line", pdf_extract, re.IGNORECASE)
        match = re.search(r"Your Royal Credit Line", pdf_extract, re.IGNORECASE)
        if match:
            statement_type = "credit_line"
            if print_logs == 'on':
                print("\nFound " + statement_type + " match")
            # Find the loan account number
            account_number = re.search(r'Your loan account number:\s*(\d{8}-\d{3})', pdf_extract, re.IGNORECASE)
            account_number = account_number.group(1) if account_number else None
    if not match:
        statement_type = "unknown"
        year = "none"
        year2 = "none"
        account_number = "none"
        return year, year2, account_number, statement_type, pdf_extract
    
    if print_all == 'logs':
        print("Account number: " + str(account_number))

    # Extract the year from the text after the matched pattern
    year_pattern = r", (20\d{2})"
    year_matches = re.findall(year_pattern, pdf_extract)
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
        print("Year Matches:" + str(year_matches))
        print("-----------")

    #return year, year2, account_number, statement_type, pypdf2_full_extract
    return year, year2, account_number, statement_type, pdf_extract

def extract_account_tables_with_camelot(pdf_path, year, year2, account_number): # Function to extract tables from the PDF using Camelot in stream mode and perform initial processing
    
    # Set pandas display to show all columns
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # Extract data from pages with camelot-py and combines
    tables_pgs = []
    tables_pg1 = camelot.read_pdf(pdf_path, flavor='stream', pages='1', edge_tol=35, column_tol=2, row_tol=4, suppress_stdout=True) # Extract pg 1 with explicit parameters
    cleaned_pg1 = [table for table in tables_pg1 if table.shape[1] >= 5]
    if len(cleaned_pg1) == 0:
        tables_pg1 = camelot.read_pdf(pdf_path, flavor='stream', pages='1', edge_tol=50, suppress_stdout=True) # When the right tables aren't found, adjust the parameters to try a better tolerance
        cleaned_pg1 = [table for table in tables_pg1 if table.shape[1] >= 5]
        if len(cleaned_pg1) == 0:
            tables_pg1 = camelot.read_pdf(pdf_path, flavor='stream', pages='1', edge_tol=100, suppress_stdout=True) # When the 5 column table isn't found, adjust for a 4 column table
            cleaned_pg1 = [table for table in tables_pg1 if table.shape[1] >= 4]

    tables_pg2p = camelot.read_pdf(pdf_path, flavor='stream', pages='2-end', edge_tol=35, column_tol=2, row_tol=4, suppress_stdout=True) # Extract all pages after 1 with alternate parameters
    cleaned_pg2p = [table for table in tables_pg2p if table.shape[1] >= 5]
    if len(cleaned_pg2p) == 0:
        tables_pg2p = camelot.read_pdf(pdf_path, flavor='stream', pages='2-end', edge_tol=22, column_tol=2, row_tol=4, suppress_stdout=True) # When the right tables aren't found, adjust the parameters to try a better tolerance
        cleaned_pg2p = [table for table in tables_pg2p if table.shape[1] >= 5]
        if len(cleaned_pg2p) == 0:
            tables_pg2p = camelot.read_pdf(pdf_path, flavor='stream', pages='2-end', edge_tol=9, column_tol=2, row_tol=4, suppress_stdout=True) # When the right tables aren't found, adjust the parameters to try a better tolerance
            cleaned_pg2p = [table for table in tables_pg2p if table.shape[1] >= 4]

    # Combines tables series
    tables_pgs.extend(cleaned_pg1)
    tables_pgs.extend(cleaned_pg2p)

    if print_logs == 'on': # Print the parsing report
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
        header_index = table.df[table.df.apply(lambda row: all(header in " ".join(row) for header in headers), axis=1)].index
        if print_logs == 'on':
            print("Header index: " + str(header_index))
        if len(header_index) > 0:
            table.df = table.df.iloc[header_index[0]:] # If the header row is found, set the DataFrame to rows starting from that index
            try: # Fix for PDF extracts that concat Date & Description columns
                is_match = table.df.iloc[0].isin(["Date\nDescription"]) # Check if the first row contains the specified string
                if any(is_match):
                    col_index = int(is_match[is_match].index[0]) # Get the column index where the match is True
                    table.df.insert(col_index, 'Date', table.df[col_index]) # Duplicate the column to the left by inserting it at the same index
                    table.df.loc[~table.df['Date'].str.contains(r'\n'), 'Date'] = "" # In the date column, replace any values that don't have "\n" in them with " "
                    table.df['Date'] = table.df['Date'].str.replace(r'\n.*', '', regex=True) # In the date column, trim "\n"
                    # In the description column, trim anything from the "\" of the first "\n"
                    is_match = table.df.iloc[0].isin(["Date\nDescription"]) # Check if the first row contains the specified string
                    col_index = int(is_match[is_match].index[0]) # Get the column index where the match is True
                    table.df[col_index] = table.df[col_index].str.replace(r'.*\n', '', regex=True)
                    if print_logs == 'on':
                        print("---------")
                        print("Fix for concatenated Date & Description columns:")
                        print(table.df)
                else:
                    is_match = table.df.iloc[0].str.contains(r'.*\nDate', case=False, na=False)
                    if is_match.any():
                        col_index = int(is_match[is_match].index[0]) # Get the column index where the match is True
                        table.df[col_index] = table.df[col_index].str.replace(r'.*\nDate', 'Date', regex=True)
                        if print_all == 'on':
                            print("---------")
                            print("Fix for concatenated Date & vertical first column:")
                            print(table.df)
            except ValueError: # Handle the case where the column name is not found
                print(r"Column 'Date\nDescription' not found")
        else:
            continue # If the header row is not found, skip this table

        # Identify the columns that contain any of the required headers
        selected_columns = []
        for col in table.df.columns:
            if any(string in table.df[col].values for string in headers):
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

        table.df.insert(1, "Account #", "") # Insert new column for Account Numbers

        # Fixes multiline concatenation - loops through the DataFrame starting from the second row
        i = 0
        while i < len(table.df):
            current_desc = table.df.iloc[i, 2].strip()
            credit = table.df.iloc[i, 3].strip()
            debit = table.df.iloc[i, 4].strip()

            # Start of a potential multi-line transaction
            if current_desc and not credit and not debit:
                full_desc = [current_desc]
                full_date = [table.df.iloc[i, 0].strip()]
                start_idx = i
                found_end = False

                j = i + 1
                while j < len(table.df):
                    desc = table.df.iloc[j, 2].strip()
                    credit = table.df.iloc[j, 3].strip()
                    debit = table.df.iloc[j, 4].strip()

                    if desc:
                        full_desc.append(desc)
                        full_date.append(table.df.iloc[j, 0].strip())

                    # If we find a row with credit or debit — it's the end of this transaction
                    if credit or debit:
                        # Merge descriptions and date
                        table.df.iloc[j, 2] = " | ".join(full_desc)
                        table.df.iloc[j, 0] = " ".join([d for d in full_date if d])
                        table.df.iloc[j, 1] = account_number
                        found_end = True

                        # Clear all earlier lines
                        for k in range(start_idx, j):
                            table.df.iloc[k, 2] = ''
                            table.df.iloc[k, 0] = ''
                            table.df.iloc[k, 1] = ''
                        break

                    j += 1

                i = j + 1 if found_end else i + 1
            else:
                # Single-line transaction: just assign account number
                if current_desc:
                    table.df.iloc[i, 1] = account_number
                i += 1

        # Final cleanup
        table.df = table.df[table.df.iloc[:, 2].str.strip() != '']
        table.df = table.df[table.df.iloc[:, 2].str.strip() != 'No activity for this period']
        
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
    table_areas = ['50, 595, 360, 35']
    table_areas2 = ['50, 608, 360, 35']
    column_bounds = ['96, 125, 306']
    
    tables_pg1 = camelot.read_pdf(pdf_path, flavor='stream', table_areas=table_areas, columns=column_bounds, pages='1', edge_tol=1, column_tol=0, row_tol=6, suppress_stdout=True, split_text=True) # Extract pg 1 with explicit parameters
    cleaned_pg1 = [table for table in tables_pg1 if table.shape[1] >= 3]
    tables_pg2p = camelot.read_pdf(pdf_path, flavor='stream', table_areas=table_areas2, columns=column_bounds, pages='2-end', edge_tol=1, column_tol=0, row_tol=6, suppress_stdout=True, split_text=True) # Extract all pages after 1 with alternate parameters
    cleaned_pg2p = [table for table in tables_pg2p if table.shape[1] >= 3]

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

        header_index = table.df[table.df.apply(lambda row: row.str.contains("ACTIVITY DESCRIPTION", case=True).any(), axis=1)].index # Find the index of the header row
        if print_all == 'on':
            print("Header index: " + str(header_index))
        if len(header_index) > 0:
            table.df = table.df.iloc[header_index[0]:] # If the header row is found, set the DataFrame to rows starting from that index
        else:
            continue # If the header row is not found, skip this table

        # Identify the columns that contain any of the required headers
        selected_columns = []
        date_flag = False
        for col in table.df.columns:
            if table.df[col].apply(lambda x: any(substring in str(x) for substring in cc_headers)).any():
                new_col_name = f"{table.df[col].iloc[0]}"
                #if 'DATE' in new_col_name: # Look for date in the new column name
                if 'TRANSACTION' in new_col_name:
                    if not date_flag:
                        new_col_name = 'Date'
                        date_flag = True # Sets the flag after the first date occurance, ignoring the second
                    else:
                        table.df.drop(columns=[col], inplace=True)
                        continue # Skips to the next interation of the loop
                elif 'POSTING' in new_col_name:
                    table.df.drop(columns=[col], inplace=True)
                    continue
                elif 'DESCRIPTION' in new_col_name: # Look for description in the new column name
                    new_col_name = 'Description'
                elif 'AMOUNT ($)' in new_col_name: # Look for description in the new column name
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

        # Convert the 'Date' column to the new format only if it matches the original format
        table.df['Date'] = table.df['Date'].apply(lambda x: datetime.strptime(x, '%b %d, %Y').strftime('%d %b, %Y') if re.match(r'^[A-Z]{3} \d{2}, \d{4}$', x) else x)

        table.df.insert(1, "Account #", "") # Insert new column for Account Numbers
        table.df.insert(4, "Debit ($)", "") # Insert new debit column for splitting negative values to
        table.df.insert(5, "Balance ($)", "") # Insert a balance column to match the account statements for consistency in processing

        # Loops through the DataFrame to fix multiline concatenation, split out negative values to the debit column, add the account number
        table.df = table.df.reset_index(drop=True)
        table.df['Description'] = table.df['Description'].str.replace('\n', ' | ')

        i = 0
        while i < len(table.df):
            desc = table.df.loc[i, 'Description'].strip()
            credit = table.df.loc[i, 'Credit ($)'].strip()
            debit = table.df.loc[i, 'Debit ($)'].strip()

            # Only proceed if we have a valid transaction (has an amount)
            if desc and (credit or debit):
                # Fix negative credit to debit
                if credit.startswith('-'):
                    table.df.loc[i, 'Debit ($)'] = credit[1:]
                    table.df.loc[i, 'Credit ($)'] = ''

                table.df.loc[i, 'Account #'] = account_number
                base_index = i
                full_desc = [desc]
                full_date = [table.df.loc[i, 'Date'].strip()]

                j = i + 1
                while j < len(table.df):
                    desc_j = table.df.loc[j, 'Description'].strip()
                    credit_j = table.df.loc[j, 'Credit ($)'].strip()
                    debit_j = table.df.loc[j, 'Debit ($)'].strip()

                    # If the next row has no amount but has a description, it's part of this transaction
                    if desc_j and not credit_j and not debit_j:
                        full_desc.append(desc_j)
                        full_date.append(table.df.loc[j, 'Date'].strip())
                        # Clear out the extra row
                        table.df.loc[j, 'Description'] = ''
                        table.df.loc[j, 'Date'] = ''
                        table.df.loc[j, 'Account #'] = ''
                    else:
                        # We hit a new transaction (or end), stop merging
                        break

                    j += 1

                # Apply merged description and date
                table.df.loc[base_index, 'Description'] = ' | '.join(full_desc)
                table.df.loc[base_index, 'Date'] = ' '.join([d for d in full_date if d])

                i = j  # Continue from where we left off
            else:
                i += 1

        table.df = table.df[table.df.iloc[:, 2].str.strip() != ''] # Drop rows where the description is empty
        table.df = table.df[table.df.iloc[:, 2].str.strip() != 'No activity for this period'] # Drop rows where the description indicates no activity
        table.df = table.df[table.df.iloc[:, 2].str.strip() != 'SUBTOTAL OF MONTHLY ACTIVITY'] # Drop rows where the description indicates no activity
        table.df = table.df[~table.df.iloc[:, 2].astype(str).str.contains(' - CO-APPLICANT', na=False)] # Drop rows where the description contains "CO-APPLICANT"

        dataframes.append(table.df) # Append the DataFrame to the list

        if print_page == 'on': # Loop through each table and print its content
            print("")
            print(f"---------Page {table.page} processed data:----------")
            print(table.df)
            print(f"-------Page {table.page} processed data End:--------")
        
    return dataframes

def extract_credit_line_tables_with_camelot(pdf_path, year, year2, account_number):
    
    # Set pandas display to show all columns
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)

    # Extract data from pages with camelot-py and combines
    tables_pgs = []
    
    tables_pg1 = camelot.read_pdf(pdf_path, flavor='stream', pages='1', edge_tol=1, column_tol=0, row_tol=6, suppress_stdout=True) # Extract pg 1 with explicit parameters
    cleaned_pg1 = [table for table in tables_pg1 if table.shape[1] >= 3]
    tables_pg2p = camelot.read_pdf(pdf_path, flavor='stream', pages='2-end', edge_tol=1, column_tol=0, row_tol=6, suppress_stdout=True) # Extract all pages after 1 with alternate parameters
    cleaned_pg2p = [table for table in tables_pg2p if table.shape[1] >= 3]

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

        header_index = table.df[table.df.apply(lambda row: row.str.contains("ACTIVITY DESCRIPTION", case=True).any(), axis=1)].index # Find the index of the header row
        if print_all == 'on':
            print("Header index: " + str(header_index))
        if len(header_index) > 0:
            table.df = table.df.iloc[header_index[0]:] # If the header row is found, set the DataFrame to rows starting from that index
        else:
            continue # If the header row is not found, skip this table

        # Identify the columns that contain any of the required headers
        selected_columns = []
        date_flag = False
        for col in table.df.columns:
            if table.df[col].apply(lambda x: any(substring in str(x) for substring in cc_headers)).any():
                new_col_name = f"{table.df[col].iloc[0]}"
                if 'DATE' in new_col_name: # Look for date in the new column name
                    if not date_flag:
                        new_col_name = 'Date'
                        date_flag = True # Sets the flag after the first date occurance, ignoring the second
                    else:
                        table.df.drop(columns=[col], inplace=True)
                        continue # Skips to the next interation of the loop
                elif 'DESCRIPTION' in new_col_name: # Look for description in the new column name
                    new_col_name = 'Description'
                elif 'AMOUNT ($)' in new_col_name: # Look for description in the new column name
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

        # Convert the 'Date' column to the new format only if it matches the original format
        table.df['Date'] = table.df['Date'].apply(lambda x: datetime.strptime(x, '%b %d, %Y').strftime('%d %b, %Y') if re.match(r'^[A-Z]{3} \d{2}, \d{4}$', x) else x)

        table.df.insert(1, "Account #", "") # Insert new column for Account Numbers
        table.df.insert(4, "Debit ($)", "") # Insert new debit column for splitting negative values to
        table.df.insert(5, "Balance ($)", "") # Insert a balance column to match the account statements for consistency in processing

        # Loops through the DataFrame starting from the second row to fix multiline concatenation, split out negative values to the debit column, add the account number
        table.df = table.df.reset_index(drop=True)
        table.df['Description'] = table.df['Description'].str.replace('\n', ' | ')
        for i in range(1, len(table.df)):
            if re.search(r"\b\d{23}\b", table.df.loc[i, 'Description']):
                if i + 1 in table.df.index and table.df.loc[i+1, 'Description'].startswith('Foreign Currency'): # Searches for Foreign currency lines and merges them with their transaction line
                    table.df.loc[i, 'Description'] = table.df.loc[i, 'Description'] + ' | ' + table.df.loc[i+1, 'Description']
                    table.df.loc[i+1, 'Description'] = ''
                table.df.loc[i-1, 'Description'] = table.df.loc[i-1, 'Description'] + ' | ' + table.df.loc[i, 'Description']
                table.df.loc[i, 'Description'] = ''
            if table.df.loc[i, 'Description'].startswith('CASH BACK') and i + 1 in table.df.index and re.search(r"\d{10}", table.df.loc[i+1, 'Description']): # Searches for cash back credits and merges them with thier transaction line
                table.df.loc[i, 'Description'] = table.df.loc[i, 'Description'] + ' | ' + table.df.loc[i+1, 'Description']
                table.df.loc[i+1, 'Description'] = ''
            if table.df.loc[i, 'Description'].startswith('OFFER RONA') and i + 1 in table.df.index and re.search(r".*\d{10,}", table.df.loc[i+1, 'Description']): # Searches for cash back credits and merges them with thier transaction line
                table.df.loc[i, 'Description'] = table.df.loc[i, 'Description'] + ' | ' + table.df.loc[i+1, 'Description']
                table.df.loc[i+1, 'Description'] = ''
            if table.df.loc[i, 'Date'].strip() and table.df.loc[i, 'Credit ($)'].strip(): # Check if the date and amount rows are empty for the current row
                table.df.iloc[i, 1] = account_number # Add in the account number
            if table.df.loc[i, 'Credit ($)'].startswith('-'): #Check if the value is negative
                table.df.loc[i, 'Debit ($)'] = table.df.loc[i, 'Credit ($)'][1:] # Copy the value to the debit column and make it positive
                table.df.loc[i, 'Credit ($)'] = '' # Erase the original value
        table.df = table.df[table.df.iloc[:, 2].str.strip() != ''] # Drop rows where the description is empty
        table.df = table.df[table.df.iloc[:, 2].str.strip() != 'No activity for this period'] # Drop rows where the description indicates no activity
        table.df = table.df[table.df.iloc[:, 2].str.strip() != 'SUBTOTAL OF MONTHLY ACTIVITY'] # Drop rows where the description indicates no activity

        dataframes.append(table.df) # Append the DataFrame to the list

        if print_page == 'on': # Loop through each table and print its content
            print("")
            print(f"---------Page {table.page} processed data:----------")
            print(table.df)
            print(f"-------Page {table.page} processed data End:--------")
        
    return dataframes

def extract_credit_line_tables_with_pdfplumber(pdf_path, year, year2, account_number):
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)

    dataframes = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:

            # Extract tables with adjusted tolerance settings
            raw_table = page.extract_table({"vertical_strategy": "explicit", 
                                            "explicit_vertical_lines": [45,84,258,375,457,520,598],
                                            "horizontal_strategy": "text", 
                                            "text_y_tolerance": 4, 
                                            "text_x_tolerance": 4, 
                                            "intersection_x_tolerance": 2, 
                                            "edge_min_length": 20
                                            })

            if not raw_table:
                continue  # Skip if no table is found

            df = pd.DataFrame(raw_table)
            df = df.dropna(how='all')  # Drop empty rows

            # Find header row
            header_index = None
            for i, row in df.iterrows():
                if any(re.search(r"Interest/Fees/Insurance", str(cell), re.IGNORECASE) for cell in row):
                    header_index = i
                    break

            if header_index is None:
                continue  # Skip if no header found

            df = df.iloc[header_index:]  # Keep only rows from header onward

            df.columns = df.iloc[0]  # Set new column headers
            df = df[1:].reset_index(drop=True)  # Remove header row from data

            # Rename important columns
            column_mapping = {
                "Date": "Date",
                "Description": "Description",
                "Interest/Fees/Insurance($)": "Interest/Fees/Insurance ($)",
                "Withdrawals($)": "Credit ($)",
                "Payments($)": "Debit ($)",
                "Balanceowing($)": "Balance ($)"
            }
            df.rename(columns={col: column_mapping[col] for col in df.columns if col in column_mapping}, inplace=True)

            # Remove unwanted rows (e.g., summary totals)
            #df = df[~df.apply(lambda row: "Balance ($)" in " ".join(row.astype(str)), axis=1)]

            # Append year to dates
            if "Date" in df.columns:
                df["Date"] = df["Date"].apply(lambda x: f"{x}, {year}" if x.strip() and x != "Date" else x)

                # Add a space before the month if missing (e.g., "22Nov" → "22 Nov")
                df["Date"] = df["Date"].apply(lambda x: re.sub(r"(\d{1,2})([A-Z][a-z]{2})", r"\1 \2", x) if isinstance(x, str) else x)

                # Convert to a standard date format
                df["Date"] = df["Date"].apply(
                    lambda x: datetime.strptime(x, "%b %d, %Y").strftime("%d %b, %Y") if re.match(r"^[A-Z]{3} \d{2}, \d{4}$", x) else x
                )

            # Drop empty rows
            df = df[df["Description"].str.strip() != ""]
            df = df[df["Balance ($)"].str.strip() != "1of"]
            # df = df[df["Date"].str.strip() != ""]
            df = df[df["Interest/Fees/Insurance ($)"].str.strip() != "inthisstatementforyourreco"]

            # Fixes multiline concatenation - loops through the DataFrame
            df = df.reset_index(drop=True)  # Ensure index is continuous
            rows_to_remove = []  # Track rows to delete
            for i in range(1, len(df)):  # Start from row 1 (skip header
                if pd.isna(df.at[i, "Date"]) or str(df.at[i, "Date"]).strip() == "":  # If the date is missing
                    df.at[i - 1, "Description"] += " | " + df.at[i, "Description"] # Append description to previous row

                    # Merge other columns (keep the non-null value)
                    for col in df.columns:
                        if col not in ["Description", "Date"] and pd.notna(df.at[i, col]):
                            df.at[i - 1, col] = df.at[i, col]

                    rows_to_remove.append(i)  # Mark row for deletion

            df = df.drop(rows_to_remove).reset_index(drop=True) # Remove processed rows

            # Merge multiline descriptions
            df["Description"] = df["Description"].str.replace("\n", " | ")

            # Insert additional columns
            df.insert(1, "Account #", account_number)  # Insert Account Number

            # Merge the two columns into "Credit ($)", prioritizing non-null values
            df["Credit ($)"] = df["Interest/Fees/Insurance ($)"].fillna(0) + df["Credit ($)"].fillna(0)

            # Drop the old column
            df = df.drop(columns=["Interest/Fees/Insurance ($)"])

            dataframes.append(df)

            if print_page == 'on': # Loop through each table and print its content
                print("")
                print(f"---------Processed data:----------")
                print(df)
                print(f"-------Processed data End:--------")
        
    return dataframes

def post_extraction_processing(dataframes): # Handles additional formatting of full dataframe series to clean the data once its been standardized and combined
    
    for df in dataframes:
        if df.index.duplicated().any():
            print("Duplicate index values found in a Dataframe")

    dataframes = [df.reset_index(drop=True) for df in dataframes] # Reset the index of each DataFrame

    data = pd.concat(dataframes, ignore_index=True) # Concatenate all dataframes into a single DataFrame
    
    if pd.isna(data.iloc[0, -1]): # Drop the last column of NaN values
        data = data.drop(data.columns[-1], axis=1)
    data = data.replace('', np.nan) # Replace empty strings with NaN
    headers_set1 = ["Date", re.escape(".*"), "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"]
    headers_set2 = [r"*DATE", re.escape(".*"), "ACTIVITY DESCRIPTION", "AMOUNT ($)", re.escape(".*"), re.escape(".*")]
    headers_set3 = ["Date", re.escape(".*"), "Description", "Interest/Fees/Insurance ($)", "Credit ($)", "Deposits ($)", "Balance ($)"]
    
    for headers_to_exclude in [headers_set1, headers_set2, headers_set3]:
        for header in headers_to_exclude:
            data = data[~data.apply(lambda row: header in " ".join(row.astype(str)), axis=1)]

    data.columns = ["Date", "Account #", "Description", "Credit ($)", "Debit ($)", "Balance ($)"]

    data["Date"].fillna(method='ffill', inplace=True) # Forward-fill missing dates in the "Date" column

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
            #statement_year, statement_year2, statement_acct_num, statement_type = pypdf_extract_from_pdf(pdf_path)
            statement_year, statement_year2, statement_acct_num, statement_type, pdf_extract = pdfplumber_extract_from_pdf(pdf_path)
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
                #continue
            elif statement_type == 'credit':
                dataframes_camelot = extract_credit_tables_with_camelot(pdf_path, statement_year, statement_year2, statement_acct_num)
                #continue
            elif statement_type == 'credit_line':
                #dataframes_camelot = extract_credit_line_tables_with_camelot(pdf_path, statement_year, statement_year2, statement_acct_num)
                dataframes_camelot = extract_credit_line_tables_with_pdfplumber(pdf_path, statement_year, statement_year2, statement_acct_num)
                #pass
            else:
                if print_logs == 'on':
                    print("\nNo statement type")
                continue
            if dataframes_camelot:
                if print_logs == 'on':
                    print(f"Processed {pdf_path}\n")
                all_dataframes.extend(dataframes_camelot)
            else:
                if print_logs == 'on':
                    print(f"Didn't process {pdf_path}")
                not_processed.append(pdf_path)
        except Exception as e:
            logging.error("Didn't process: %s", pdf_path)
            if print_logs == 'on':
                print(f"A pdfplumber error occurred processing the file: {pdf_path} |", e)
            if print_trace == 'on':
                traceback.print_exc()
                print(pdf_path)

    if all_dataframes: # Data extract post-processing cleanup
        combined_data = post_extraction_processing(all_dataframes)

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") # Calculate current timestamp

        csv_path = os.path.join(PDF_DIR, f"{CSV_FILE}_{timestamp}.csv")
        if save_file == 'on':
            combined_data.to_csv(csv_path, index=False)
        if print_logs == 'on' and save_file == 'on':
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
