# Royal Bank of Canada PDF To CSV Converter

A Python script to extract your transaction data from various RBC PDF statements and convert that in to a single CSV file

## Table of Contents

- [Introduction](#introduction)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Introduction

This project started out of frustration that RBC only provides CSV downloads for the last 3 months... and even that functionality I find to be spotty at times.

So I built this python script to extract data from the 6 years worth of PDFs that they store instead, converting in to a single CSV. It should work with minimal issues on most account statments and credit statments, though YMMV since I've only tested it on my scenario. Does not handle Credit Line statements - though that could be built in, I just didn't find the need for it.

If you fork the project, give me a shout and let me know - it would be interesting to see how people improve this spaghetti.

## Getting Started

Clone or download this repository to your local machine, cd to the directory.
 
Copy the 'example-mysecrets.py' file to 'mysecrets.py' in the repository root. Replace 'YOUR_PDF_DIRECTORY' with the path to the directory where your PDF files are located. Also, set the desired output CSV filename by replacing 'output_csv_file' - this file is created in the directory where you run the script. Use the following format:

PDF_DIR = "\\YOUR\\PDF\\DIRECTORY\\HERE\\" # Define path to access PDF statements
CSV_FILE = "extracted_data" # Define a filename for csv file

The PDF folder structure doesn't matter, the script will recursively retrieve PDFs from any subfolders inside of the directory you specify.

The script is customizeable, but should run out of the box relatively well for more standard statements. If you run in to errors, there are a series of customizable print settings you can turn on to view various output in your terminal window to diagnose issues.

Run 'python3 pdf2csv.py' to run the script. You data will be extracted to your specified file, with an additional file that indicates which PDF files were not processed (if that happens).

### Prerequisites

Leverages python 3.10.

See the requirements.txt file for the specific packages used in this script (I may have forgotten to delete a couple of unused ones). You should be able to run "pip install -r requirements.txt" to install the necessary packages in your environment.

## License

This project is license under the GPL. See the license file for info
