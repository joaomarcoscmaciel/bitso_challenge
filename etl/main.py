import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import yaml
from scripts.extract import load_csv_incremental
from scripts.transform import (
    transform_user_data, transform_fact_withdrawals, transform_fact_events,
    transform_fact_deposits, transform_dim_currency, transform_dim_interface,
    transform_dim_time, transform_dim_event_type
)
from scripts.load import save_to_postgres
from scripts.state import read_last_update_timestamp, update_last_update_timestamp
from utils.logging import setup_logging

# Load configuration
with open('config/config.yml') as file:
    config = yaml.safe_load(file)

# Setup logging
setup_logging(config['log_file'])

def run_etl_pipeline():
    """Runs the full ETL pipeline."""
    try:
        logging.info("Starting ETL pipeline...")

        # Get last update timestamp from state file
        last_update = read_last_update_timestamp()
        
        # Extract data
        user_id_df = load_csv_incremental(config['input_data_path'] + 'user_id_sample_data.csv', last_update)
        user_level_df = load_csv_incremental(config['input_data_path'] + 'user_level_sample_data.csv', last_update)
        withdrawals_df = load_csv_incremental(config['input_data_path'] + 'withdrawals_sample_data.csv', last_update)
        deposits_df = load_csv_incremental(config['input_data_path'] + 'deposit_sample_data.csv', last_update)
        events_df = load_csv_incremental(config['input_data_path'] + 'event_sample_data.csv', last_update)

        # Transform data
        fact_withdrawals_table = transform_fact_withdrawals(withdrawals_df)
        fact_deposits_table = transform_fact_deposits(deposits_df)
        fact_events_table = transform_fact_events(events_df)
        dim_user_table = transform_user_data(user_id_df, user_level_df)
        dim_currency_table = transform_dim_currency(fact_withdrawals_table, fact_deposits_table)
        dim_interface_table = transform_dim_interface(fact_withdrawals_table)

        # Get date range for Dim_Time
        min_date = min(fact_withdrawals_table['event_timestamp'].min(), fact_events_table['event_timestamp'].min())
        max_date = max(fact_withdrawals_table['event_timestamp'].max(), fact_events_table['event_timestamp'].max())
        dim_time_table = transform_dim_time(min_date, max_date)

        dim_event_type_table = transform_dim_event_type(fact_events_table)

        # Load data
        save_to_postgres(fact_withdrawals_table, 'fact_withdrawals')
        save_to_postgres(fact_deposits_table, 'fact_deposits')
        save_to_postgres(fact_events_table, 'fact_events')
        save_to_postgres(dim_user_table, 'dim_user')
        save_to_postgres(dim_currency_table, 'dim_currency')
        save_to_postgres(dim_interface_table, 'dim_interface')
        save_to_postgres(dim_time_table, 'dim_time')
        save_to_postgres(dim_event_type_table, 'dim_event_type')

        # Update the last update timestamp
        update_last_update_timestamp(fact_withdrawals_table, fact_deposits_table, fact_events_table)
        
        logging.info("ETL pipeline completed successfully.")
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")

if __name__ == "__main__":
    run_etl_pipeline()

