import pytest
from unittest.mock import MagicMock
from app.file_processor import process_files

# Testcases  for the file load functionality.
@pytest.fixture
def mock_db_connection(mocker):
    # Fixture to provide a mock database connection.
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


@pytest.mark.parametrize("file_name, read_function, extension", [
    ("input_dataset_csv", "read_csv", ".csv"),
    ("input_dataset_json", "read_json", ".json"),
    ("input_dataset_xml", "read_xml", ".xml"),
])
# Test processing of CSV, JSON, and XML files , present in input directory
def test_process_files_various_formats(mocker, mock_db_connection, file_name, read_function, extension):
    
    mock_conn, _ = mock_db_connection

    #  Mock file system
    mocker.patch("os.path.isdir", return_value=True)
    mocker.patch("os.listdir", return_value=[file_name + extension])
    mock_shutil = mocker.patch("shutil.move")

    # Mock file reader function (CSV, JSON, XML)
    mock_transactions = [{
        "transaction_uti": "10302281TRANS",
        "isin": "OM4258447851",
        "notional": 763000.0,
        "notional_currency": "GBP",
        "transaction_type": "Sell",
        "transaction_datetime": "2024-11-25T15:06:22Z",
        "exchange_rate": 0.0070956000,
        "legal_entity_identifier": "NWBV00SHMKBFN1RKWK79"
    }]

    # Mock individual file read functions called for file_processor read_csv , read_json , read_xml
    mocker.patch(f"app.file_processor.{read_function}", return_value=mock_transactions)

    # Mock process_transactions called from file_processor
    mock_process_transactions = mocker.patch("app.file_processor.process_transactions")

    # Call function the test function
    process_files("input_path", "error_path", "processed_path", mock_conn)

    # Assertions
    mock_process_transactions.assert_called_once_with(mock_transactions, mock_conn)  # Ensure it's called once with expected args
    mock_shutil.assert_called_once()  # Ensure file was moved to processed folder


# Test scenario where input directory is empty.
def test_process_files_no_files(mocker, mock_db_connection):

    mock_conn, _ = mock_db_connection

    # Mock file system
    mocker.patch("os.path.isdir", return_value=True)
    mocker.patch("os.listdir", return_value=[])

    # Mock process_transactions and patch it
    mock_process_transactions = mocker.patch("app.transaction_processor.process_transactions")

    # Call function test function
    process_files("input_path", "error_path", "processed_path", mock_conn)

    # Ensure `process_transactions` was NOT called as there's not file at input directory
    mock_process_transactions.assert_not_called()  # Ensure it's not called

#  Test scenario where input directory does not exist.
def test_process_files_invalid_directory(mocker, mock_db_connection):

    mock_conn, _ = mock_db_connection

    # Mock file system (invalid directories)
    mocker.patch("os.path.isdir",
                 side_effect=lambda path: path != "input_path")

    # Mock process_transactions
    mock_process_transactions = mocker.patch(
        "app.transaction_processor.process_transactions")

    # Call function
    process_files("input_path", "error_path", "processed_path", mock_conn)

    # Ensure error message was logged
    # It should NOT be called since directory is invalid
    mock_process_transactions.assert_not_called()

# Test scenario where an unsupported file type is present.
def test_process_files_unsupported_file(mocker, mock_db_connection):

    mock_conn, _ = mock_db_connection

    # Mock file system
    mocker.patch("os.path.isdir", return_value=True)
    mocker.patch("os.listdir", return_value=["input_dataset.txt"])  # Unsupported extension

    # Mock logging to capture warnings
    mock_logging = mocker.patch("logging.warning")

    # Call function
    process_files("input_path", "error_path", "processed_path", mock_conn)

    # Ensure warning was logged
    mock_logging.assert_called_with("Unsupported file_name for input file : input_dataset.txt")

#  Test scenario where processing transactions fails and file moves to errored folder.
def test_process_files_error_handling(mocker, mock_db_connection):

    mock_conn, _ = mock_db_connection

    # Mock file system
    mocker.patch("os.path.isdir", return_value=True)
    mocker.patch("os.listdir", return_value=["input_dataset_csv.csv"])
    mock_shutil = mocker.patch("shutil.move")

    # Mock CSV reader
    mock_transactions = [{
        "transaction_uti": "10302281TRANS",
        "isin": "OM4258447851",
        "notional": 763000.0,
        "notional_currency": "GBP",
        "transaction_type": "Sell",
        "transaction_datetime": "2024-11-25T15:06:22Z",
        "exchange_rate": 0.0070956000,
        "legal_entity_identifier": "NWBV00SHMKBFN1RKWK79"
    }]

    #mocker.patch(f"app.file_processor.{read_function}", return_value=mock_transactions)
    
    # Mock `process_transactions` to raise an error
    mocker.patch("app.transaction_processor.process_transactions",side_effect=Exception("DB Error"))

    # Call function
    process_files("input_path", "error_path", "processed_path", mock_conn)

    # Ensure file is moved to error folder
    mock_shutil.assert_called_once()
