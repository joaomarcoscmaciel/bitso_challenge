import logging

def check_null_values(df, critical_fields, table_name):
    """Checks for null values in critical fields of a DataFrame."""
    try:
        for field in critical_fields:
            null_count = df[field].isnull().sum()
            if null_count > 0:
                logging.warning(f"Field '{field}' has {null_count} null values in {table_name}.")
            else:
                logging.info(f"Field '{field}' has no null values in {table_name}.")
    except Exception as e:
        logging.error(f"Error during event type check in {table_name}: {e}")
