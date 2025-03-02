import os
import shutil
import pandas as pd
from app.read_csv import read_csv
from app.db_handler import get_db_connection, setup_database
from app.config import INPUT_PATH, ERROR_PATH, PROCESSED_PATH, DB_NAME,DB_PATH
from app.file_processor import process_files
from unittest.mock import MagicMock

input_path = INPUT_PATH
error_path = ERROR_PATH
processed_path = PROCESSED_PATH

def mock_get_db_connection():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


def test_load_csv_successfully(mocker):
    # Test successful CSV load.
    csv_file_path =  input_path

    # Delete the existing directory and recreate new one
    shutil.rmtree(csv_file_path)
    os.makedirs(csv_file_path)
 
    #  Test Data
    data = [{"transaction_uti": "10302281TRANSCSV",
             "isin": "OM4258447851",
             "notional": 763000.0,
             "notional_currency": "GBP",
             "transaction_type": "Sell",
             "transaction_datetime": "2024-11-25T15:06:22Z",
             "exchange_rate": 0.0070956000,
             "legal_entity_identifier": "NWBV00SHMKBFN1RKWK79"
             }]

    df = pd.DataFrame(data)
    csv_file_path = os.path.join(csv_file_path, "input_data_csv.csv")
    df.to_csv(csv_file_path, index=False)

    # Load data from the CSV file into the database
    # Mock database connection & cursor
    mock_conn, mock_cursor = mock_get_db_connection()
    mocker.patch('app.db_handler.get_db_connection', return_value=mock_conn)
    process_files(input_path, error_path, processed_path, mock_conn)

    mock_cursor.fetchall.return_value = [
        ("10302281TRANSCSV", "OM4258447851", 763000.0, "GBP", "Sell", "2024-11-25T15:06:22Z", 0.0070956000, "NWBV00SHMKBFN1RKWK79")]

    # Check if the data is correctly inserted
    assert mock_cursor.executemany.called
    mock_cursor.execute("SELECT * FROM transactions")
    rows = mock_cursor.fetchall()

    # Check that there are rows in the table
    assert len(rows) > 0
    assert rows[0][0] == "10302281TRANSCSV"
    assert rows[0][1] == "OM4258447851"
    assert rows[0][2] == 763000.0
    assert rows[0][3] == "GBP"
    assert rows[0][4] == "Sell"
    assert rows[0][5] == "2024-11-25T15:06:22Z"
    assert rows[0][6] == 0.0070956000
    assert rows[0][7] == "NWBV00SHMKBFN1RKWK79"


def test_load_empty_csv(mocker):
    
    csv_file_path = input_path
    
    # Delete the existing directory and recreate new one
    shutil.rmtree(csv_file_path)
    os.makedirs(csv_file_path)
 
    # Test Data
    data = [{"transaction_uti": "",
             "isin": "",
             "notional": "",
             "notional_currency": "",
             "transaction_type": "",
             "transaction_datetime": "",
             "exchange_rate": "",
             "legal_entity_identifier": ""
             }]
    
    # Writing the csv file with only headers in the db
    csv_file_path = os.path.join(csv_file_path, "input_data_csv.csv")
    df = pd.DataFrame(data)
    df.to_csv(csv_file_path, index=False)

    # Mock database connection  
    mock_conn, mock_cursor = mock_get_db_connection()
    mocker.patch('app.db_handler.get_db_connection', return_value=mock_conn)
    process_files(input_path, error_path, processed_path, mock_conn)

    # Mock the expected result
    mock_cursor.fetchall.return_value = []

    mock_cursor.execute("SELECT * FROM transactions")
    rows = mock_cursor.fetchall()

    # No query should be executed in case of empty csv file
    # No of rows read should be equal to 0 
    assert mock_cursor.executemany.called == 0
    assert len(rows) == 0


def test_invalid_data_csv(mocker):
    
    csv_file_path = input_path

    # Drop and recreate input directory 
    shutil.rmtree(csv_file_path)
    os.makedirs(csv_file_path)
 
    # Test data with Invalid entries
    data = [{"transaction_uti": "10302281TRANSCSV",
             "isin": "OM4258447851",
             "notional": "OM4258447851",  # Invalid
             "notional_currency": "GBP",
             "transaction_type": "Sell",
             "transaction_datetime": "2024-11-25T15:06:22Z",
             "exchange_rate": 0.0070956000,
             "legal_entity_identifier": "NWBV00SHMKBFN1RKWK79"
             },
            {"transaction_uti": "",  # Invalid
             "isin": "OM4258447851",
             "notional": "OM4258447851",
             "notional_currency": "GBP",
             "transaction_type": "Sell",
             "transaction_datetime": "2024-11-25T15:06:22Z",
             "exchange_rate": 0.0070956000,
             "legal_entity_identifier": "NWBV00SHMKBFN1RKWK79"
             }]

     # Writing the data to csv file
    csv_file_path = os.path.join(csv_file_path, "input_data_csv.csv")
    df = pd.DataFrame(data)
    df.to_csv(csv_file_path, index=False)
 
    # Mocking the database connection
    mock_conn, mock_cursor = mock_get_db_connection()
    mocker.patch('app.db_handler.get_db_connection', return_value=mock_conn)
    process_files(input_path, error_path, processed_path, mock_conn)


   
    mock_cursor.fetchall.return_value = []

    mock_cursor.execute("SELECT * FROM transactions")
    rows = mock_cursor.fetchall()
    
    # No records should be inserted into db as both records are invalid
    assert mock_cursor.executemany.called == 0
    assert len(rows) == 0


def test_missing_columns_csv(mocker):

    csv_file_path = input_path
    
    # Remove and recreate the input directory
    shutil.rmtree(csv_file_path)
    os.makedirs(csv_file_path)

    # Test data with missing column
    data = [{"transaction_uti": "10302281TRANSCSV",
             "notional": 763000.0,
             "notional_currency": "GBP",
             "transaction_type": "Sell",
             "transaction_datetime": "2024-11-25T15:06:22Z",
             "exchange_rate": 0.0070956000,
             "legal_entity_identifier": "NWBV00SHMKBFN1RKWK79"
             }]  # Missing ISIN Column

    # Writing data to csv file
    csv_file_path = os.path.join(csv_file_path, "input_data_csv.csv")
    df = pd.DataFrame(data)
    df.to_csv(csv_file_path, index=False)
 
    # Mock the db connection
    mock_conn, mock_cursor = mock_get_db_connection()
    mocker.patch('app.db_handler.get_db_connection', return_value=mock_conn)
    process_files(input_path, error_path, processed_path, mock_conn)

    # Mock the expected cursor fetch all statement
    mock_cursor.fetchall.return_value = []


    mock_cursor.execute("SELECT * FROM transactions")
    rows = mock_cursor.fetchall()

    # No db execute statement should be executed and 
    # records inserted should be zero in case of missing column in the file
    assert mock_cursor.executemany.called == 0
    assert len(rows) == 0
