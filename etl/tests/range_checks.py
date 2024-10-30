import logging

def check_range(df, column, min_value, max_value, table_name):
    """Checks if values in a column fall within a specified range."""
    try:
        out_of_range = df[(df[column] < min_value) | (df[column] > max_value)]
        if not out_of_range.empty:
            logging.warning(f"{len(out_of_range)} rows in {table_name} have {column} values outside the range {min_value} to {max_value}.")
        else:
            logging.info(f"All values in {column} of {table_name} are within the specified range.")
    except Exception as e:
        logging.error(f"Error during range check in {table_name}: {e}")

