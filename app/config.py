# config.py
# Configuration file for Cardono data processor application
# - Database connection settings
# - Logging configuration
# - File paths and directories

# Database settings
DB_NAME = "Transaction_Reporting.db"
DB_PATH = "data"

# File path for Input files.
# Processed_Path - for files which are processed successfuly without any errors.
# Error_Path - for files which are not processed and resulted in errors.
INPUT_PATH = "data/input"
ERROR_PATH = "data/errored"
PROCESSED_PATH = "data/processed"
