import os
import pandas as pd
import numpy as np #Import numpy to handle NaN values
import camelot
import re
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
year = ""
account_num = ""

# Function to extract tables from the PDF using Camelot in stream mode
def extract_tables_with_camelot(pdf_path):
    # Read all pages using Camelot in stream mode
    tables = camelot.read_pdf(pdf_path, flavor='stream', pages='1-end', edge_tol=34)
    
    dataframes = []

    for page_num, table in enumerate(tables, start=1):
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

        # Determine if the "Date" column needs to be shifted
        if ("Date" not in table.df.iloc[:, 0].values) and ("Date" in table.df.iloc[:, 1].values):
        # if "Date" not in table.df.columns:
            # If "Date" is not in the first column, shift all columns left, replace NaNs with blank strings,
            # and drop the last column (index 5) from the DataFrame
            table.df = table.df.shift(periods=-1, axis=1)
            table.df.fillna('', inplace=True)
            table.df = table.df.drop(table.df.columns[5], axis=1)

        # Find the index of "Opening Balance" to remove it
        opening_balance_index = table.df[table.df.apply(lambda row: "Opening Balance" in " ".join(row), axis=1)].index
        if len(opening_balance_index) > 0:
            table.df = table.df.drop(opening_balance_index[0])

        # Find the index of "Closing Balance" to remove rows from it and after
        end_index = table.df.loc[table.df.apply(lambda row: "Closing Balance" in " ".join(row), axis=1)].index
        if not end_index.empty:
            table.df = table.df.loc[:end_index.values[0] - 1]

        # Append the DataFrame to the list
        dataframes.append(table.df)

        #print(f"Processed page {page_num}")

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
        combined_data = combined_data.dropna(how='all')
        
        # Remove rows containing headers
        headers_to_exclude = ["Date", "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"]
        for header in headers_to_exclude:
            combined_data = combined_data[~combined_data.apply(lambda row: header in " ".join(row.astype(str)), axis=1)]

        # Set the DataFrame columns using the headers from the first page
        combined_data.columns = headers_to_include
        #combined_data.columns = headers_to_include + ["Extra Date"] # Use when the data is getting shifted from multiple PDFs

        # Forward-fill missing dates in the "Date" column
        combined_data['Date'].fillna(method='ffill', inplace=True)

        #print(combined_data)

        csv_path = os.path.join(PDF_DIR, f"{CSV_FILE}.csv")
        combined_data.to_csv(csv_path, index=False)
        print(f"Data saved to {csv_path}")
    else:
        print("No data extracted from any PDFs.")

# Replace 'YOUR_PDF_DIRECTORY' and 'output_csv_file' with your desired values in the mysecrets.py file.
process_pdfs()