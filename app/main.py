import config
import logging
import sqlite3
from db_handler import setup_database, get_db_connection
from display_data import display_transactions_pretty
from file_processor import process_files


# The main entry point of the script.
# This function is responsible for initializing the program.
# Step 1: Create the database and table (if not already created)
# Step 2: Process the files for loading into database
# Step 3: Display the data after loading it

def main():
    # Step 1:
    setup_database()

    input_path = config.INPUT_PATH
    errored_path = config.ERROR_PATH
    processed_path = config.PROCESSED_PATH
    try:
        conn = get_db_connection()
        # Step 2:
        process_files(input_path, errored_path, processed_path, conn)

        # Step 3:
        display_transactions_pretty(conn)
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
