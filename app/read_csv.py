import os
import csv
import logging

# Parameter : file_path (str): Input directory and filename for file to loaded
# Reads a CSV file and returns a list of transactions.
# Each transaction is represented as a dictionary.
# Checks if all the required columns are present in file before processing further
def read_csv(file_path):
   
    if not os.path.exists(file_path) or os.stat(file_path).st_size == 0:
        raise ValueError(f"CSV file {file_path} is empty or missing.")

    transactions = []

    required_columns = ["transaction_uti", "isin", "notional", "notional_currency",
                        "transaction_type", "transaction_datetime", "exchange_rate", "legal_entity_identifier"]

    try:
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            logging.info(f"Started reading file:{file_path}")
            reader = csv.DictReader(csvfile)
            missing_columns = [
                col for col in required_columns if col not in reader.fieldnames]

            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

            for row in reader:
                transactions.append(row)
        return transactions

    except ValueError as e:
        raise ValueError(f"Error processing csv file {file_path}:{e}")
