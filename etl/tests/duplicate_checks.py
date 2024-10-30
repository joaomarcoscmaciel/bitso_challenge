import logging

def check_duplicates(df, primary_key, table_name):
    """Checks for duplicate records in a DataFrame based on the primary key."""
    try:
        duplicate_count = df.duplicated(subset=[primary_key]).sum()
        if duplicate_count > 0:
            logging.warning(f"{duplicate_count} duplicate records found in {table_name}.")
        else:
            logging.info(f"No duplicates found in {table_name}.")
    except Exception as e:
        logging.error(f"Error during event type check in {table_name}: {e}")
