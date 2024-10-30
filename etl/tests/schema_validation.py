import logging

def validate_schema(df, expected_columns, table_name):
    """Validates the schema of a DataFrame."""
    try:
        actual_columns = list(df.columns)

        if actual_columns == expected_columns:
            logging.info(f"{table_name} schema is valid.")
        else:
            logging.warning(f"{table_name} schema is INVALID.")
            logging.warning("Expected:", expected_columns)
            logging.warning("Found:", actual_columns)
    except Exception as e:
        logging.error(f"Error during event type check in {table_name}: {e}")