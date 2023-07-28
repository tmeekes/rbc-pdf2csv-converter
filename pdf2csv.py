import os
import pandas as pd
import io
import pytesseract
import fitz
import re
import camelot
from secrets import PDF_DIR, CSV_FILE

# Function to get a list of PDF files in a directory and its subdirectories
def get_pdf_files_recursive(directory):
    pdf_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    return pdf_files

# Function to extract tables from the PDF using Camelot
def extract_tables_with_camelot(pdf_path):
    tables = camelot.read_pdf(pdf_path, flavor='stream')
    dataframes = [table.df for table in tables]
    return dataframes

# Function to preprocess the PDF and extract text using OCR
def preprocess_pdf_and_extract_text(pdf_path):
    extracted_text = ""
    pdf_document = fitz.open(pdf_path)
    for page_num in range(pdf_document.page_count):
        page = pdf_document[page_num]
        image_blob = page.get_pixmap().tobytes()
        image = Image.open(io.BytesIO(image_blob)).convert("RGB")
        extracted_text_page = pytesseract.image_to_string(image)
        extracted_text += extracted_text_page
        print(extracted_text_page)  # Print extracted text for each page
    pdf_document.close()
    return extracted_text

# Function to extract data from the OCR output and return a DataFrame
def extract_data_from_ocr(pdf_path):
    extracted_text = preprocess_pdf_and_extract_text(pdf_path)
    
    # Define header pattern (adjust as per your PDF's header format)
    header_pattern = r"(Date|Transaction Date)\s+(Amount)\s+(Description)"
    header_match = re.search(header_pattern, extracted_text, re.IGNORECASE)

    # Define data pattern (adjust as per your PDF's data format)
    data_pattern = r"(\d{4}-\d{2}-\d{2})\s+(\$\d+\.\d+)\s+(.+)"

    if header_match:
        # Extract data rows using the data pattern
        data_matches = re.findall(data_pattern, extracted_text, re.IGNORECASE)
        data = [match for match in data_matches]

        # Create DataFrame with the extracted data
        data_df = pd.DataFrame(data, columns=["Date", "Amount", "Description"])
        return data_df
    else:
        return pd.DataFrame()

# Function to process PDFs and save data to CSV
def process_pdfs():
    pdf_files = get_pdf_files_recursive(PDF_DIR)
    for pdf_path in pdf_files:
        dataframes_camelot = extract_tables_with_camelot(pdf_path)
        if dataframes_camelot:
            # Process the dataframes_camelot to convert tables to CSV
            for i, table_df in enumerate(dataframes_camelot):
                csv_path = os.path.join(PDF_DIR, f"table_{i + 1}.csv")
                table_df.to_csv(PDF_DIR, index=False)
        else:
            data_df = extract_data_from_ocr(pdf_path)
            if not data_df.empty:
                # Convert OCR data to CSV
                PDF_DIR = os.path.join(PDF_DIR, os.path.basename(pdf_path)[:-4] + ".csv")
                data_df.to_csv(PDF_DIR, index=False)
            else:
                print(f"No data extracted from {pdf_path}")

if __name__ == "__main__":
    process_pdfs()