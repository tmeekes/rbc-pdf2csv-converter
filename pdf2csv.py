import os
import pandas as pd
import camelot
from mysecrets import PDF_DIR, CSV_FILE

# Function to get a list of PDF files in a directory and its subdirectories
def get_pdf_files_recursive(PDF_DIR):
    pdf_files = []
    for root, _, files in os.walk(PDF_DIR):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    return pdf_files

# Function to extract tables from the PDF using Camelot in stream mode
def extract_tables_with_camelot(pdf_path, start_string="Details of your account activity", headers_to_include=None):
    if headers_to_include is None:
        headers_to_include = ["Date", "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"]
    
    # Read the first page with edge_tol=34 and other pages with edge_tol=0
    tables_page1 = camelot.read_pdf(pdf_path, flavor='stream', pages='1')
    tables_page2_onwards = camelot.read_pdf(pdf_path, flavor='stream', pages='2-end')
    
    dataframes = []
    table_num = 1

    for table in tables_page1:
        if table.df.empty:
            continue

        # Find the index of the start_string to filter data below it
        start_index = table.df[table.df.apply(lambda row: start_string in " ".join(row), axis=1)].index

        if len(start_index) > 0:
            # If start_string is found, set the DataFrame to rows starting from that index
            df = table.df.iloc[start_index[0] + 1:]
        else:
            df = table.df

        # Find the index of "Closing Balance" to remove rows after it
        end_index = df[df.apply(lambda row: "Closing Balance" in " ".join(row), axis=1)].index

        if len(end_index) > 0:
            # If "Closing Balance" is found, set the DataFrame to rows until that index
            df = df.iloc[:end_index[0] + 1:]
            # Drop the rows after "Closing Balance"
            df = df.drop(index=df.index[end_index[0] + 1:])

        # Drop the first column (index 0) from the DataFrame
#        df = df.drop(df.columns[0], axis=1)

        # Append the DataFrame to the list
        dataframes.append(df)

        # Print the content of the table
        print(f"Table {table_num} (Page 1) - Coordinates: ({table._bbox[0]}, {table._bbox[1]}, {table._bbox[2]}, {table._bbox[3]})")
        print(df)
        print("\n")

        # # Check if all headers_to_include are present in the table's DataFrame
        # if set(headers_to_include).issubset(table.df.values[0]):
        #     # Find the index of the start_string to filter data below it
        #     start_index = table.df[table.df.apply(lambda row: start_string in " ".join(row), axis=1)].index

        #     if len(start_index) > 0:
        #         # If start_string is found, set the DataFrame to rows starting from that index
        #         df = table.df.iloc[start_index[0] + 1:]
        #     else:
        #         df = table.df

        #     # Find the index of "Closing Balance" to remove rows after it
        #     end_index = df[df.apply(lambda row: "Closing Balance" in " ".join(row), axis=1)].index

        #     if len(end_index) > 0:
        #         # If "Closing Balance" is found, set the DataFrame to rows until that index
        #         df = df.iloc[:end_index[0] + 1:]
        #         # Drop the rows after "Closing Balance"
        #         df = df.drop(index=df.index[end_index[0] + 1:])

        #     # Append the DataFrame to the list
        #     dataframes.append(df)

        #     # Print the content of the table
        #     print(f"Table {table_num}:")
        #     print(df)
        #     print("\n")

        table_num += 1

    for table in tables_page2_onwards:
        if table.df.empty:
            continue

        # Check if the specific string is present in the table
        #if exclude_string in table.df.values:
            #continue  # Skip this table and proceed to the next one

        # Find the index of the start_string on other pages
        start_index = table.df[table.df.apply(lambda row: start_string in " ".join(row), axis=1)].index

        if len(start_index) > 0:
            # If start_string is found, set the DataFrame to rows starting from that index
            table.df = table.df.iloc[start_index[0] + 1:]

        # Find the index of "Closing Balance" to remove rows after it
        end_index = table.df[table.df.apply(lambda row: "Closing Balance" in " ".join(row), axis=1)].index

        if len(end_index) > 0:
            # If "Closing Balance" is found, set the DataFrame to rows until that index
            table.df = table.df.iloc[:end_index[0] + 1:]
            # Drop the rows after "Closing Balance"
            table.df = table.df.drop(index=table.df.index[end_index[0] + 1:])

        # Append the DataFrame to the list
        dataframes.append(table.df)

        # Print the content of the table
        print(f"Table {table_num}:")
        print(table.df)
        print("\n")

        table_num += 1

        # Print the number of tables found on each page
        print(f"Number of tables found on Page 1: {len(tables_page1)}")
        print(f"Number of tables found on Page 2 onwards: {len(tables_page2_onwards)}")

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
            print(f"No data extracted from {pdf_path}")

    if all_dataframes:
        # Concatenate all dataframes into a single DataFrame
        combined_data = pd.concat(all_dataframes, ignore_index=True)

        # Set the DataFrame columns using the headers from the first page
        combined_data.columns = combined_data.iloc[0]

        # Remove the first row, which contains the headers (already used for columns)
        combined_data = combined_data.iloc[1:]

        csv_path = os.path.join(PDF_DIR, f"{CSV_FILE}.csv")
        combined_data.to_csv(csv_path, index=False)
        print(f"Data saved to {csv_path}")
    else:
        print("No data extracted from any PDFs.")

# Replace 'YOUR_PDF_DIRECTORY' and 'output_csv_file' with your desired values.
process_pdfs()
