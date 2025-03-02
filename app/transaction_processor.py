import logging
import pandas as pd
from db_handler import get_db_connection
from itertools import islice

# Schema for converting the dataframe to standard transaction table format
transaction_schema = {
    'transaction_uti': 'string',
    'isin': 'string',
    'notional': 'float64',
    'notional_currency': 'string',
    'transaction_type': 'string',
    'transaction_datetime': 'string',
    'exchange_rate': 'float64',
    'legal_entity_identifier': 'string'
}

# Input parameter : transactions - list of dictionaries containing all the transaction read from the file
#                   conn - connection object to connect to database
# Step 1: Validates the transactions read from file with transaction_validations function.
# Step 2: Perform GBP to EUR conversion for notional amount
# Step 3: Performs CreateOrupdate on Database table
def process_transactions(transactions, conn):

    # Step 1
    df = transaction_validations(transactions)

    # Debugging: Print the size of DataFrame after validation
    logging.info(f"Transactions after validation: {len(df)}")

    if df.empty:
        logging.info("No valid transactions to process.")
        return  # Exit function early

    cursor = conn.cursor()

    # Step 2
    df["amount_eur"] = df["notional"]*df["exchange_rate"]

    # Step 3
    transactions_list = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO transactions (transaction_uti,isin,notional,notional_currency,transaction_type,transaction_datetime,
        exchange_rate,legal_entity_identifier,amount_eur)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(transaction_uti) 
        DO UPDATE SET 
            isin= excluded.isin,
            notional= excluded.notional,
            notional_currency= excluded.notional_currency,
            transaction_type= excluded.transaction_type,
            transaction_datetime= excluded.transaction_datetime,
            exchange_rate= excluded.exchange_rate,
            legal_entity_identifier= excluded.legal_entity_identifier,
            amount_eur= excluded.amount_eur;
    """

    for batch in batched(transactions_list, 1000):
        try:
            cursor.executemany(query, batch)  # Bulk operation
            cursor.executemany(query, transactions_list)
            transactions_count = cursor.rowcount
            conn.commit()
            logging.info(
                f"Number of transactions successfully inserted/updated into the database: {transactions_count}")

        except Exception as e:
            logging.info("Error inserting data:", e)
            raise e


# 1 - Checks if notional and exchange rate fields are numeric , remove all rows where these fields are NAN or Null.
# 2 - Remove rows where transaction_uti is empty string or Null.
def transaction_validations(transactions):

    df = pd.DataFrame(transactions)
    df['notional'] = pd.to_numeric(df['notional'], errors='coerce')
    df['exchange_rate'] = pd.to_numeric(df['exchange_rate'], errors='coerce')

    # Logging the records with bad data
    bad_data = df[df['notional'].isna() | df['exchange_rate'].isna() | df['transaction_uti'].isin(['', 'Null']) | df['transaction_uti'].isna()]
    if not bad_data.empty:
        logging.warning("transaction_uti with Bad Data: \n%s", bad_data[['transaction_uti', 'isin', 'legal_entity_identifier']].to_string(index=False))

    # filtering the dataframe from bad data
    df_filtered = df.dropna(
        subset=['notional', 'exchange_rate', 'transaction_uti'])
    df_filtered = df_filtered[~df_filtered['transaction_uti'].isin([""])]

    # Converting to standard dataframe with column datatype defined in schema
    transformed_df = df_filtered.astype(transaction_schema)

    return transformed_df

# Divides the input list into batched for writing to db in batches
def batched(iterable, n):
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch
