import os
import xml.etree.ElementTree as ET

# Parameter : file_path (str): Input directory and filename for file to loaded
# Reads an XML file and returns a list of transactions.
# Each transaction is represented as a dictionary.

def read_xml(file_path):

    if not os.path.exists(file_path) or os.stat(file_path).st_size == 0:
        raise ValueError(f"XML file {file_path} is empty or missing.")

    transactions = []
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        for trans in root.findall("transaction"):
            transactions.append({
                "transaction_uti": trans.find("transaction_uti").text,
                "isin": trans.find("isin").text,
                "notional": trans.find("notional").text,
                "notional_currency": trans.find("notional_currency").text,
                "transaction_type": trans.find("transaction_type").text,
                "transaction_datetime": trans.find("transaction_datetime").text,
                "exchange_rate": trans.find("exchange_rate").text,
                "legal_entity_identifier": trans.find("lei").text
            })
        return transactions

    except ET.ParseError as e:
        raise ValueError(f"Error parsing XML file {file_path}: {e}")
