import pandas as pd
from tabulate import tabulate

# Module for displaying data in tabular form to user
def display_transactions_pretty(conn):

    df = pd.read_sql_query("SELECT * FROM transactions Limit 20", conn)

    # Print formatted table
    print(tabulate(df, headers='keys', tablefmt='psql'))
