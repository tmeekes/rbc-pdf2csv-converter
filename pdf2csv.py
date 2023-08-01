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

# Function to extract tables from the PDF using Camelot in stream mode
def extract_tables_with_camelot(pdf_path):
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

        # If Date isn't the first column, shift all columns left replace NaNs with blank strings (Removes the vertical left column string)
        if ("Date" not in table.df.iloc[:, 0].values) and ("Date" in table.df.iloc[:, 1].values):
            table.df = table.df.shift(periods=-1, axis=1)

            table.df.fillna('', inplace=True)

            # Drop the last column (index 5) from the DataFrame
            table.df = table.df.drop(table.df.columns[5], axis=1)

        # Find the index of "Opening Balance" to remove it
        opening_balance_index = table.df[table.df.apply(lambda row: "Opening Balance" in " ".join(row), axis=1)].index
        if len(opening_balance_index) > 0:
            # If "Opening Balance" is found, set the DataFrame to rows until that index. *** For some reason, it's using a row well below "Closing Balance"... manually adjusted, but need to revisit to clean up
            #table.df = table.df.iloc[opening_balance_index[0]]
            table.df = table.df.drop(opening_balance_index[0])

        # Find the index of "Closing Balance" to remove rows after it
        end_index = table.df[table.df.apply(lambda row: "Closing Balance" in " ".join(row), axis=1)].index

        if len(end_index) > 0:
            # If "Closing Balance" is found, set the DataFrame to rows until that index
            table.df = table.df.iloc[:end_index[0] + 1]
            print(f"Closing Balance was found on row {end_index[0]}.")
            # Drop the rows after "Closing Balance"
            table.df = table.df.drop(index=table.df.index[end_index[0] + 1:])

        # Append the DataFrame to the list
        dataframes.append(table.df)

        #print(f"Processed page 1")

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
        end_index = table.df[table.df.apply(lambda row: "Closing Balance" in " ".join(row), axis=1)].index

        if len(end_index) > 0:
            # If "Closing Balance" is found, set the DataFrame to rows until that index. *** For some reason, it's using a row well below "Closing Balance"... manually adjusted, but need to revisit to clean up
            #table.df = table.df.iloc[:end_index[0] - 4]
            table.df = table.df.iloc[:end_index[0] - 2]

        # Drop the last column (index 5) from the DataFrame
        #table.df = table.df.drop(table.df.columns[5], axis=1)

        # Append the DataFrame to the list
        dataframes.append(table.df)

        #print(f"Processed page 2+")

    return dataframes

# Function to process PDFs and save data to CSV
def process_pdfs():
    pdf_files = get_pdf_files_recursive(PDF_DIR)
    all_dataframes = []

    for pdf_path in pdf_files:
        dataframes_camelot = extract_tables_with_camelot(pdf_path)
        if dataframes_camelot:
            #print(f"Using Camelot in stream mode to extract data from {pdf_path}")
            all_dataframes.extend(dataframes_camelot)
        else:
            continue

    if all_dataframes:
        # Concatenate all dataframes into a single DataFrame
        combined_data = pd.concat(all_dataframes, ignore_index=True)
        
        # Set the DataFrame columns using the headers from the first page
        #combined_data.columns = headers_to_include
        #combined_data.columns = headers_to_include + ["Extra Date"] # Use when the data is getting shifted from multiple PDFs

        csv_path = os.path.join(PDF_DIR, f"{CSV_FILE}.csv")
        combined_data.to_csv(csv_path, index=False)
        print(f"Data saved to {csv_path}")
    else:
        print("No data extracted from any PDFs.")

# Replace 'YOUR_PDF_DIRECTORY' and 'output_csv_file' with your desired values in the mysecrets.py file.
process_pdfs()