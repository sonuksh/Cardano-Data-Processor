import os
import json
import logging

# Parameter : file_path (str): Input directory and filename for file to loaded
# Reads a JSON file and returns a list of transactions.
# Each transaction is represented as a dictionary.
def read_json(file_path):

    transaction_schema = {
        "transaction_uti": "transaction_uti",
        "isin": "isin",
        "notional": "notional",
        "notional_currency": "notional_currency",
        "transaction_type": "transaction_type",
        "transaction_datetime": "transaction_datetime",
        "exchange_rate": "exchange_rate",
        "lei": "legal_entity_identifier"
    }

    if not os.path.exists(file_path) or os.stat(file_path).st_size == 0:
        raise ValueError(f"JSON file {file_path} is empty or missing.")

    try:
        with open(file_path, "r", encoding="utf-8") as jsonfile:
            logging.info(f"Started reading file:{file_path}")
            transactions = []
            loaded_data = json.load(jsonfile)
            loaded_transactions = loaded_data['transactions']
            for data in loaded_transactions:
                missing_columns = transaction_schema.keys() - data.keys()
                if missing_columns:
                    logging.warning(
                        f"Missing required property: {', '.join(missing_columns)} in the file  {file_path}")
                    logging.warning(
                        f"Missing required property on row number: {data.index} in the file  {file_path}")
                else:
                    transaction = {transaction_schema[key]: value for key, value in data.items() if key in transaction_schema}
                    transactions.append(transaction)
        return transactions

    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding JSON file {file_path}: {e}")
