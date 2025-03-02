import pandas as pd
from unittest.mock import MagicMock
from app.db_handler import get_db_connection
from app.transaction_processor import process_transactions, transaction_validations, batched

# Set up the Mock DB connection
def mock_get_db_connection():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn , mock_cursor


def test_process_transactions(mocker):

    transactions = [
        {
            "transaction_uti": "TXN1023",
            "isin": "US12345",
            "notional": 150.0,
            "notional_currency": "USD",
            "transaction_type": "BUY",
            "transaction_datetime": "2024-02-26T12:00:00",
            "exchange_rate": 1.1,
            "legal_entity_identifier": "LEI123"
        }
    ]

    df = pd.DataFrame(transactions)

    # Mocking the transaction validation function
    mocker.patch(
        'app.transaction_processor.transaction_validations', return_value=df)

    # Mocking the database connection and cursor

    mock_conn ,mock_cursor= mock_get_db_connection()
 
    # Calling the test function
    process_transactions(df, mock_conn)

    # Assertions to check if the queries were executed
    # Assertions
    assert mock_cursor.executemany.called
    assert mock_conn.commit.called  # Ensure commit was called after the insert/update
    # Ensure one row were passed for insert
    assert len(mock_conn.cursor().executemany.call_args[0][1]) == 1

    # Check if the SQL query is correct in the call
    query_called = mock_conn.cursor().executemany.call_args[0][0]
    assert "INSERT INTO transactions" in query_called
    assert "ON CONFLICT(transaction_uti)" in query_called


def test_transaction_validations():
    # Test transaction validation without database connection.
    transactions = [
        {"transaction_uti": "TXN1", "isin": "US12345", "notional": 100, "notional_currency": "USD",
         "transaction_type": "BUY", "transaction_datetime": "2024-02-26T12:00:00", "exchange_rate": 1.1,
         "legal_entity_identifier": "LEI123"},
        {"transaction_uti": "", "isin": "US12346", "notional": "200", "notional_currency": "GBP",
         "transaction_type": "SELL", "transaction_datetime": "2024-02-26T12:30:00", "exchange_rate": "0.9",
         "legal_entity_identifier": "LEI124"},  # Invalid (empty transaction_uti)
        {"transaction_uti": "TXN100", "isin": "US12347", "notional": "US12347", "notional_currency": "GBP",
         "transaction_type": "SELL", "transaction_datetime": "2024-02-26T12:30:00", "exchange_rate": "GBP",
         "legal_entity_identifier": "LEI124"}  # Invalid (Notional Value and Exchange rate)
    ]

    valid_df = transaction_validations(transactions)
    assert len(valid_df) == 1  # Only one valid transaction should remain


def test_batched():
    data = list(range(10))
    batches = list(batched(data, 3))  # Split into batches of 3
    assert len(batches) == 4  # Expect 4 batches (3+3+3+1)
    # First batch should contain first 3 elements
    assert batches[0] == (0, 1, 2)
    assert batches[-1] == (9,)  # Last batch should contain remaining element
