import os
import pandas as pd
import io
import pytesseract
import fitz
import re
import tabula 
from PIL import Image
from mysecrets import PDF_DIR, CSV_FILE

# Function to get a list of PDF files in a directory and its subdirectories
def get_pdf_files_recursive(PDF_DIR):
    pdf_files = []
    for root, _, files in os.walk(PDF_DIR):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    return pdf_files

# Function to extract tables from the PDF using Tabula-py
import fitz  # Import PyMuPDF

def extract_tables_with_tabula(pdf_path, areas):
    pdf_document = fitz.open(pdf_path)
    all_tables = []

    num_pages = pdf_document.page_count  # Get the total number of pages

    for page_num, area in enumerate(areas):
        # Check if the page number is within the range
        if page_num >= num_pages:
            print(f"Warning: Page {page_num + 1} does not exist in {pdf_path}. Skipping...")
            continue

        tables = tabula.read_pdf(pdf_path, pages=page_num + 1, guess=True, multiple_tables=True, area=area)
        dataframes = [table for table in tables if table.shape[0] > 1]  # Filter out single-row tables
        all_tables.extend(dataframes)

    pdf_document.close()
    return all_tables

# Function to preprocess the PDF and extract text using OCR
def preprocess_pdf_and_extract_text(pdf_path, ignore_left_points=34):
    extracted_text = ""
    pdf_document = fitz.open(pdf_path)

    for page_num in range(pdf_document.page_count):
        page = pdf_document[page_num]

        # For the first page, adjust the clipping area to ignore the left-hand margin
        if page_num == 0:
            rect = page.rect
            rect.x0 += ignore_left_points
            page.set_cropbox(rect)

        image_blob = page.get_pixmap().tobytes()
        image = Image.open(io.BytesIO(image_blob)).convert("RGB")
        extracted_text_page = pytesseract.image_to_string(image)
        extracted_text += extracted_text_page + "\n"

    pdf_document.close()
    return extracted_text

# Function to extract data from the OCR output and return a DataFrame
def extract_data_from_ocr(pdf_path, start_string="Details of your account activity"):
    extracted_text = preprocess_pdf_and_extract_text(pdf_path)

    # Find the index of the start_string to filter data below it
    start_index = extracted_text.find(start_string)
    if start_index == -1:
        print(f"Start string '{start_string}' not found in {pdf_path}.")
        return pd.DataFrame(columns=["Date", "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"])

    # Extract data from the start_string and onward
    extracted_text = extracted_text[start_index:]
    
    # Define header pattern (adjust as per your PDF's header format)
    header_pattern = r"Date\s+Description\s+Withdrawals \(\$.*\)\s+Deposits \(\$.*\)\s+Balance \(\$.*\)"
    header_match = re.search(header_pattern, extracted_text, re.IGNORECASE)

    # Define data pattern (adjust as per your PDF's data format)
    data_pattern = r"(\d{4}-\d{2}-\d{2})\s+(.+?)\s+(-?\$\d+(?:,\d{3})*(?:\.\d{2})?)\s+(-?\$\d+(?:,\d{3})*(?:\.\d{2})?)\s+(-?\$\d+(?:,\d{3})*(?:\.\d{2})?)"

    if header_match:
        # Extract data rows using the data pattern
        data_matches = re.findall(data_pattern, extracted_text, re.IGNORECASE)
        data = [match for match in data_matches]

        # Create DataFrame with the extracted data
        data_df = pd.DataFrame(data, columns=["Date", "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"])

        # Remove rows with missing data and reset the DataFrame index
        data_df.dropna(subset=["Date", "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"], inplace=True)
        data_df.reset_index(drop=True, inplace=True)

        # Split the "Withdrawals ($)" and "Deposits ($)" columns to remove potential concatenation
        data_df["Withdrawals ($)"] = data_df["Withdrawals ($)"].apply(lambda x: x.split()[0])
        data_df["Deposits ($)"] = data_df["Deposits ($)"].apply(lambda x: x.split()[0])

        return data_df
    else:
        # Return an empty DataFrame with the correct column names
        return pd.DataFrame(columns=["Date", "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"])

# Function to process PDFs and save data to CSV
def process_pdfs(start_string=None):
    
    # Define the expected headers of the table
    expected_headers = ["Date", "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"]
    
    pdf_files = get_pdf_files_recursive(PDF_DIR)
    all_dataframes = []
    for pdf_path in pdf_files:
        dataframes_tabula = extract_tables_with_tabula(pdf_path, expected_headers)
        if dataframes_tabula:
            print(f"Using Tabula to extract data from {pdf_path}")
            all_dataframes.extend(dataframes_tabula)
        else:
            print(f"Using OCR to extract data from {pdf_path}")
            data_df = extract_data_from_ocr(pdf_path)
            if not data_df.empty:
                all_dataframes.append(data_df)
            else:
                print(f"No data extracted from {pdf_path}")

    if all_dataframes:
        # Combine all dataframes into a single DataFrame
        combined_data = pd.concat(all_dataframes, ignore_index=True)

        # Filter rows below the start string (if specified) only if "Date" column exists
        if start_string and "Date" in combined_data.columns:
            start_row = combined_data[combined_data["Date"].str.contains(start_string)].index
            if not start_row.empty:
                combined_data = combined_data.iloc[start_row[0] + 1 :]

        # Save the DataFrame to CSV with header (column names as the first row)
        csv_path = os.path.join(PDF_DIR, f"{CSV_FILE}.csv")
        combined_data.to_csv(csv_path, index=False, header=True)
        print(f"Data saved to {csv_path}")
    else:
        print("No data extracted from any PDFs.")

# Replace 'YOUR_PDF_DIRECTORY' and 'output_csv_file' with your desired values.
process_pdfs()