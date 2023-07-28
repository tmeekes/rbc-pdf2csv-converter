import os
import re
import pandas as pd
import io
import pytesseract
import fitz
import camelot
from PyPDF2 import PdfReader
from PIL import Image

from secrets import PDF_DIR
from secrets import CSV_FILE

# Define the directory path where your PDFs are located and the name of the csv file (pulled from secrets)
pdf_directory = PDF_DIR
csv_file = CSV_FILE

# Define the header and data pattern for use in recognition
# header_pattern = r"Date\s+Description\s+Withdrawals ($)\s+Deposits ($)\s+Balance ($)"
header_pattern = r"Date"
data_pattern = r""

# Function to preprocess the PDF and extract text using OCR
def preprocess_pdf_and_extract_text(pdf_path):
    extracted_text = ""
    pdf_document = fitz.open(pdf_path)
    for page_num in range(pdf_document.page_count):
        page = pdf_document[page_num]
        image_blob = page.get_pixmap().tobytes()
        image = Image.open(io.BytesIO(image_blob)).convert("RGB")
        extracted_text += pytesseract.image_to_string(image)
    pdf_document.close()
    return extracted_text

def extract_header_from_ocr(ocr_text):
    header_match = re.search(header_pattern, ocr_text)
    if header_match:
        header = header_match.group()
        return header.strip().split()
    else:
        return None

def extract_columns_from_ocr(ocr_text):
    # Extract the header row from the OCR output
    header = extract_header_from_ocr(ocr_text)

    # If header extraction was successful, find the starting index of each column
    if header:
        col_indices = [ocr_text.index(header[col]) for col in header]
        col_indices.append(len(ocr_text))  # Add the end index

        # Extract columns of data based on the indices
        columns = [ocr_text[col_indices[i]:col_indices[i + 1]] for i in range(len(col_indices) - 1)]
        return columns
    else:
        return None

def extract_data_from_ocr(pdf_path):
    extracted_text = preprocess_pdf_and_extract_text(pdf_path)

    # Extract columns of data from the OCR output
    columns = extract_columns_from_ocr(extracted_text)

    if columns:
        # Convert columns to a Pandas DataFrame
        data_df = pd.DataFrame([col.split() for col in columns], columns=columns[0].split())
        return data_df
    else:
        return None

def extract_tables_with_camelot(pdf_path):
    tables = camelot.read_pdf(pdf_path, flavor='stream')
    dataframes = [table.df for table in tables]
    return dataframes

# Process all PDFs in the directory and its subdirectories
def process_pdfs(pdf_directory, csv_file):
    # Process PDFs with Camelot (Table Extraction)
    pdf_files = get_pdf_files_recursive(pdf_directory)
    for pdf_path in pdf_files:
        dataframes_camelot = extract_tables_with_camelot(pdf_path)
        if dataframes_camelot:
            # Process the dataframes_camelot to convert tables to CSV
            # ... (rest of the code) ...
        else:
            # Process PDFs with OCR (existing approach)
            data_df = extract_data_from_ocr(pdf_path)
            if not data_df.empty:
                # Convert OCR data to CSV
                # ... (rest of the code) ...
            else:
                print(f"No data extracted from {pdf_path}")

    # Concatenate all DataFrames into a single DataFrame (if there are multiple PDFs)
    if len(data_frames) > 0:
        combined_df = pd.concat(data_frames, ignore_index=True)

        # Save the combined DataFrame to a CSV file in the specified pdf directory
        csv_file_path = os.path.join(pdf_directory, csv_file)
        combined_df.to_csv(csv_file_path, index=False)
    else:
        print("No data extracted from PDFs in the directory and its subdirectories.")

# Call the function to process all PDFs in the directory and its subdirectories
process_pdfs(pdf_directory, csv_file)