import sqlite3
import os
import config

db_name = config.DB_NAME
db_path = config.DB_PATH
db_path = os.path.join(db_path, db_name)


def setup_database():

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_uti TEXT PRIMARY KEY,
        isin TEXT,
        notional REAL,
        notional_currency TEXT,
        transaction_type TEXT,
        transaction_datetime TEXT,
        exchange_rate REAL,
        legal_entity_identifier TEXT,
        amount_eur REAL
 
    );
    ''')

    conn.commit()
    conn.close()



def get_db_connection():

    conn = sqlite3.connect(db_path)
    
    return conn
