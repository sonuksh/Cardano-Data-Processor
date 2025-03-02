import os
import sqlite3
from app.db_handler import setup_database
from app.db_handler import get_db_connection
from app.config import DB_PATH as dbpath, DB_NAME as dbname


def test_setup_database():
    # Delete the database file if it exists from previous runs
    db_name = dbname
    db_path = dbpath
    db_path = os.path.join(db_path, db_name)

    if os.path.exists(db_path):
        os.remove(db_path)

    # Run the database creation function
    setup_database()

    # Check if the database file exists
    assert os.path.exists(db_path)

    # Connect to the database and check if the table exists
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='transactions';")
    table_exists = cursor.fetchone()

    assert table_exists is not None

    conn.close()

# Test for conn object creation function 
def test_get_db_connection():

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='transactions';")
    table_exists = cursor.fetchone()

    assert table_exists is not None

    conn.close()
