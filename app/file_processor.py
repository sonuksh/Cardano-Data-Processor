import logging
import os
import shutil
import re
from transaction_processor import process_transactions
from read_csv import read_csv
from read_json import read_json
from read_xml import read_xml
from datetime import datetime



# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Parameters:
# - input_path (str): The directory path where the  input files are located.
# - processed_path (str): The directory path where the files will be stored after processing.
# - error_path (str): The directory path where the files are moved in case of error.
# Step 1: Check if the path provided exist and are valid directories.
# Step 2: Checks if files are present at the input path, and processes them one by one.
# Step 3: Gets the List of transactions from the each file and sends the transactions for processing.
# Step 4: if Step 3 Success , Renames the file and moves to processed folder
# Step 5: if step 3 Failure , Renames the file and moves to errored folder
def process_files(input_path, errored_path, processed_path, conn):
    # Step 1
    if not os.path.isdir(input_path):
        logging.error(f"The path {input_path} is not a valid directory for Input files.")
        return

    if not os.path.isdir(processed_path):
        logging.warning(f"The path {processed_path} is not a valid directory Processed files.")
        os.makedirs(processed_path)
        

    if not os.path.isdir(errored_path):
        logging.warning(f"The path {errored_path} is not a valid directory Errored files.")
        os.makedirs(errored_path)
         

    # Step 2
    files = os.listdir(input_path)
    if not files:
        logging.info(f"No files found in {input_path}.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for file in files:
        name, ext = os.path.splitext(file)
        pattern = r"^input_[a-zA-Z]+_[a-zA-Z]+$"
        file_path = os.path.join(input_path, file)
        try:
            if re.match(pattern, name):
                if ext == ".csv":
                    transactions = read_csv(file_path)
                elif ext == ".json":
                    transactions = read_json(file_path)
                elif ext == ".xml":
                    transactions = read_xml(file_path)
                else:
                    logging.warning(f"Unsupported file type: {file}")
                    continue
                logging.info("File read complete. Starting transaction processing...")
                
                # Step 3
                if transactions:
                    process_transactions(transactions, conn)

                # Step 4
                new_file_name = f"processed_{name}_{timestamp}{ext}"
                shutil.move(file_path, os.path.join(
                    processed_path, new_file_name))
                logging.info(f"File: {file} processed successfully and moved to {processed_path} folder")

            else:
                logging.warning(f"Unsupported file_name for input file : {file}")

        except Exception as e:
            logging.error(e)

            # Step 5
            new_file_name = f"errored_{name}_{timestamp}{ext}"
            shutil.move(file_path, os.path.join(errored_path, new_file_name))
            logging.info(f"File {file} moved to errored folder.")
