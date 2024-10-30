import pandas as pd
import logging

def load_csv(file_path):
    """Loads a CSV file into a DataFrame."""
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Successfully loaded data from {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        raise

# Function to load only new data since the last update
def load_csv_incremental(file_path, last_update_timestamp=None):
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Successfully loaded data from {file_path}")

        # Filter only new records if last_update_timestamp is provided
        if last_update_timestamp:
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
            df = df[df['event_timestamp'] > pd.to_datetime(last_update_timestamp)]

        return df
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        raise