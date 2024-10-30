import logging

def validate_data_types(df, expected_types, table_name):
    """Checks if columns in the DataFrame have the expected data types."""
    try:
        for column, expected_type in expected_types.items():
            if df[column].dtype != expected_type:
                logging.warning(f"Column '{column}' in {table_name} has unexpected type: {df[column].dtype} (expected {expected_type})")
        logging.info(f"Data type validation completed for {table_name}.")
    except Exception as e:
        logging.error(f"Error during data type validation in {table_name}: {e}")
