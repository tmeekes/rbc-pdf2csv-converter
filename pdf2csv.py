import os
import pandas as pd
import io
import pytesseract
import fitz
import re
import tabula
from mysecrets import PDF_DIR, CSV_FILE
from PIL import Image

# Function to get a list of PDF files in a directory and its subdirectories
def get_pdf_files_recursive(PDF_DIR):
    pdf_files = []
    for root, _, files in os.walk(PDF_DIR):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    return pdf_files

# Function to extract tables from the PDF using Tabula-py
def extract_tables_with_tabula(pdf_path):
    tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)
    dataframes = [table for table in tables if table.shape[0] > 1]  # Filter out single-row tables
    return dataframes

# Function to preprocess the PDF and extract text using OCR
def preprocess_pdf_and_extract_text(pdf_path, start_string=None, ignore_left_points=34):
    extracted_text = ""
    pdf_document = fitz.open(pdf_path)
    start_found = False

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

        if not start_found and start_string and start_string in extracted_text_page:
            start_found = True
            extracted_text = ""

        if start_found:
            extracted_text += extracted_text_page
            print(extracted_text_page)  # Print extracted text for each page

    pdf_document.close()
    return extracted_text

# Function to extract data from the OCR output and return a DataFrame
def extract_data_from_ocr(pdf_path):
    extracted_text = preprocess_pdf_and_extract_text(pdf_path)
    
    # Define header pattern (adjust as per your PDF's header format)
    header_pattern = r"Date\s+Description\s+Withdrawals \(\$.*\)\s+Deposits \(\$.*\)\s+Balance \(\$.*\)"
    header_match = re.search(header_pattern, extracted_text, re.IGNORECASE)

    # Define data pattern (adjust as per your PDF's data format)
    data_pattern = r"(\d{4}-\d{2}-\d{2})\s+(\$\d+\.\d+)\s+(.+)"

    if header_match:
        # Extract data rows using the data pattern
        data_matches = re.findall(data_pattern, extracted_text, re.IGNORECASE)
        
        # Check if the number of columns in each data row matches the header columns
        header_columns = header_match.group().split()
        data = [match for match in data_matches if len(match) == len(header_columns)]

        # Create DataFrame with the extracted data
        data_df = pd.DataFrame(data, columns=["Date", "Amount", "Description"])
        return data_df
    else:
        # Return an empty DataFrame with the correct column names
        return pd.DataFrame(columns=["Date", "Description", "Withdrawals ($)", "Deposits ($)", "Balance ($)"])

# Function to process PDFs and save data to CSV
def process_pdfs(start_string=None):
    pdf_files = get_pdf_files_recursive(PDF_DIR)
    all_dataframes = []
    for pdf_path in pdf_files:
        dataframes_tabula = extract_tables_with_tabula(pdf_path)
        if dataframes_tabula:
            all_dataframes.extend(dataframes_tabula)
        else:
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

        csv_path = os.path.join(PDF_DIR, f"{CSV_FILE}.csv")
        combined_data.to_csv(csv_path, index=False)
        print(f"Data saved to {csv_path}")
    else:
        print("No data extracted from any PDFs.")

# Replace 'YOUR_PDF_DIRECTORY' and 'output_csv_file' with your desired values.
process_pdfs(start_string="Details of your account activity")