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

        # Shift all columns left replace NaNs with blank strings (Removes the vertical left column string)
        table.df = table.df.shift(periods=-1, axis=1)
        table.df.fillna('', inplace=True)

        # Find the index of the header row
        header_index = table.df[table.df.apply(lambda row: all(header in " ".join(row) for header in headers_to_include), axis=1)].index

        if len(header_index) > 0:
            # If the header row is found, set the DataFrame to rows starting from that index
            table.df = table.df.iloc[header_index[0] + 1:]
        else:
            # If the header row is not found, skip this table
            continue

        # Find the index of "Closing Balance" to remove rows after it
        end_index = table.df[table.df.apply(lambda row: "Closing Balance" in " ".join(row), axis=1)].index

        if len(end_index) > 0:
            # If "Closing Balance" is found, set the DataFrame to rows until that index
            table.df = table.df.iloc[:end_index[0] + 1]
            # Drop the rows after "Closing Balance"
            table.df = table.df.drop(index=table.df.index[end_index[0] + 1:])

        # Drop the last column (index 5) from the DataFrame
        table.df = table.df.drop(table.df.columns[5], axis=1)

        # Append the DataFrame to the list
        dataframes.append(table.df)

        print(table.df)
        print(f"Processed page 1")
    
    # camelot.plot(tables_page1[0], kind='text')
    # plt.show(block=True)

    for table in tables_page2_onwards:
        if table.df.empty:
            print(f"Page 2+ is empty")
            continue

        # Find the index of the header row
        header_index = table.df[table.df.apply(lambda row: all(header in " ".join(row) for header in headers_to_include), axis=1)].index

        if len(header_index) > 0:
            # If the header row is found, set the DataFrame to rows starting from the row after that index
            table.df = table.df.iloc[header_index[0] + 1:]
        else:
            # If the header row is not found, skip this table
            continue

        # Find the index of "Closing Balance" to remove rows from it and after
        end_index = table.df[table.df.apply(lambda row: "Closing Balance" in " ".join(row), axis=1)].index

        # Print whether "Closing Balance" was found on this page
        print(f"'Closing Balance' found on page {table.page}: {len(end_index) > 0}")
        print(end_index[0])

        if len(end_index) > 0:
            # If "Closing Balance" is found, set the DataFrame to rows until that index
            table.df = table.df.iloc[:end_index[0]]
            # Drop the rows from "Closing Balance"
            table.df = table.df.drop(index=table.df.index[end_index[0]:])

        print(end_index[0])

        # Drop the last column (index 5) from the DataFrame
        table.df = table.df.drop(table.df.columns[5], axis=1)
        #print(table.df)

        # Append the DataFrame to the list
        dataframes.append(table.df)

        print(f"Processed page 2+")

    return dataframes

# Function to process PDFs and save data to CSV
def process_pdfs():
    pdf_files = get_pdf_files_recursive(PDF_DIR)
    all_dataframes = []

    for pdf_path in pdf_files:
        dataframes_camelot = extract_tables_with_camelot(pdf_path)
        if dataframes_camelot:
            print(f"Using Camelot in stream mode to extract data from {pdf_path}")
            all_dataframes.extend(dataframes_camelot)
        else:
            #print(f"No data extracted from {pdf_path}")
            continue

    if all_dataframes:
        # Concatenate all dataframes into a single DataFrame
        combined_data = pd.concat(all_dataframes, ignore_index=True)
        
        # Set the DataFrame columns using the headers from the first page
        combined_data.columns = headers_to_include
        #combined_data.columns = headers_to_include + ["Extra Date"] # Use when the data is getting shifted from multiple PDFs

        # # Filter the DataFrame to keep only the required columns
        # combined_data = combined_data.filter(headers_to_include)

        # # Print all dataframes in the list
        # for i, df in enumerate(all_dataframes, 1):
        #     print(f"\nDataframe {i}:")
        #     print(df)
        
        # # Print the combined_data DataFrame
        # print("\nCombined Data:")
        # print(combined_data)

        csv_path = os.path.join(PDF_DIR, f"{CSV_FILE}.csv")
        combined_data.to_csv(csv_path, index=False)
        print(f"Data saved to {csv_path}")
    else:
        print("No data extracted from any PDFs.")

# Replace 'YOUR_PDF_DIRECTORY' and 'output_csv_file' with your desired values in the mysecrets.py file.
process_pdfs()