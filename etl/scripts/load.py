import logging
from utils.db_connection import get_postgres_engine

def save_to_csv(df, file_name):
    """Saves a DataFrame to a CSV file."""
    try:
        df.to_csv(file_name, index=False)
        logging.info(f"Successfully saved to: {file_name}")
    except Exception as e:
        logging.error(f"Error saving to {file_name}: {e}")
        raise RuntimeError(f"Error saving {file_name}: {e}")

def save_to_postgres(df, table_name, if_exists='replace'):
    """Saves a DataFrame to a PostgreSQL table."""
    try:
        engine = get_postgres_engine()  # No need to pass config path if using default
        df.to_sql(table_name, engine, index=False, if_exists=if_exists)
        logging.info(f"Successfully saved {table_name} to PostgreSQL.")
    except Exception as e:
        logging.error(f"Error saving {table_name} to PostgreSQL: {e}")
        raise
