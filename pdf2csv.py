import os
import pandas as pd
import numpy as np #Import numpy to handle NaN values
import camelot
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
def extract_tables_with_camelot(pdf_path, start_string="Details of your account activity"):
    
    # Read the first page separately, all others grouped in one loop afterwards
#    columns=['42,88,317,423,553']
    tables_page1 = camelot.read_pdf(pdf_path, flavor='stream', pages='1', edge_tol=34)
    tables_page2_onwards = camelot.read_pdf(pdf_path, flavor='stream', pages='2-end')
    
    dataframes = []

    for table in tables_page1:
        if table.df.empty:
            continue

        df = table.df

        # Find the index of the header row
        header_index = df[df.apply(lambda row: all(header in " ".join(row) for header in headers_to_include), axis=1)].index

        if len(header_index) > 0:
            # If the header row is found, set the DataFrame to rows starting from that index
            df = df.iloc[header_index[0] + 1:]
        else:
            # If the header row is not found, skip this table
            continue

        # Find the index of "Closing Balance" to remove rows after it
        end_index = df[df.apply(lambda row: "Closing Balance" in " ".join(row), axis=1)].index

        if len(end_index) > 0:
            # If "Closing Balance" is found, set the DataFrame to rows until that index
            df = df.iloc[:end_index[0] + 1]
            # Drop the rows after "Closing Balance"
            df = df.drop(index=df.index[end_index[0] + 1:])

        # Drop the first column (index 0) from the DataFrame
        df = df.drop(df.columns[0], axis=1)

        # Append the DataFrame to the list
        dataframes.append(df)
    
#    camelot.plot(tables_page1[0], kind='text')
#plt.show(block=True)

    for table in tables_page2_onwards:
        if table.df.empty:
            continue

        # # Find the index of the start_string on other pages
        # start_index = table.df[table.df.apply(lambda row: start_string in " ".join(row), axis=1)].index

        # if len(start_index) > 0:
        #     # If start_string is found, set the DataFrame to rows starting from that index
        #     table.df = table.df.iloc[start_index[0] + 1:]

        # Find the index of the header row
        header_index = table.df[table.df.apply(lambda row: all(header in " ".join(row) for header in headers_to_include), axis=1)].index
        print(header_index)

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

        # Append the DataFrame to the list
        dataframes.append(table.df)

    return dataframes

# Function to process PDFs and save data to CSV
def process_pdfs(start_string=None):
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
        
        # # Set the DataFrame columns using the headers from the first page
        # combined_data.columns = headers_to_include
        combined_data.columns = headers_to_include + ["Extra Date"]

        # # Filter the DataFrame to keep only the required columns
        # combined_data = combined_data.filter(headers_to_include)

        csv_path = os.path.join(PDF_DIR, f"{CSV_FILE}.csv")
        combined_data.to_csv(csv_path, index=False)
        print(f"Data saved to {csv_path}")
    else:
        print("No data extracted from any PDFs.")

# Replace 'YOUR_PDF_DIRECTORY' and 'output_csv_file' with your desired values in the mysecrets.py file.
process_pdfs()