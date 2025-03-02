# Cardano Data Processor

## Overview
The Cardano Data Processor is a Python-based application designed to read, validate, process, and store transactions from multiple file formats (CSV, JSON, and XML) into an SQLite database. It performs data validation, currency conversion, and ensures transactional integrity using an upsert strategy.

## Features
- **Supports CSV, JSON, and XML file formats**
- **Validates transactions** before insertion into the database
- **Currency Conversion to EUR** for transaction amounts
- **Renaming and Moving file to Processed/Errored folder** after completion of file load
- **Bulk insert & update** using SQLite's `ON CONFLICT DO UPDATE`
- **Unit tests included** using `pytest`
- **Logging for debugging and error tracking**

## Project Structure
```
Cardano-Data-Processor/
│── app/
│   ├── __init__.py                     # Marks app as a package
|   ├── config.py                       # Configuration settings (DB path, logging, etc.) 
│   ├── db_handler.py                   # Handles database connections and operations
│   ├── display_data.py                 # Displays data in tabular format
│   ├── file_processor.py               # Handles file processing logic
│   ├── main.py                         # Main entry point of the application
│   ├── read_csv.py                     # Reads CSV files
│   ├── read_json.py                    # Reads JSON files
│   ├── read_xml.py                     # Reads XML files
│   ├── transaction_processor.py        # Validates and processes transaction
│── data/                               # Directory for transaction files
│   ├── input/                          # Folder for raw input files
│   ├── errored/                        # Folder for errored or invalid files
│   ├── processed/                      # Folder for successfully processed files
│── tests/
│   ├── test_db_handler.py              # Unit tests for database operations
│   ├── test_file_processor.py          # Unit tests for file processing
│   ├── test_file_load_csv.py           # Unit tests for end to end loading of csv file
│   ├── test_transaction_processor.py   # Unit tests for processing transactions
│── pytest.ini                          # Pytest configuration file
│── requirements.txt                    # Dependencies for the project
│── README.md                           # Project documentation (this file)
```

## Setup Instructions
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/my_project.git
   cd  cardano-data-processor


2. Install dependencies:
    pip install -r requirements.txt

3. Run the app:
    python app/main.py

4. Run the tests:
    python -m pytest
    
 
---
**Maintained by:** [Sonu Kumari Sharma]