import pandas as pd
import logging

def check_user_id_consistency(fact_df, dim_df, fact_table, dim_table):
    """Checks if all user_ids in the fact table exist in the dimension table."""
    try:
        unmatched_users = fact_df[~fact_df['user_id'].isin(dim_df['user_id'])]
        if len(unmatched_users) > 0:
            logging.warning(f"Unmatched user IDs found in {fact_table} with {dim_table}.")
        else:
            logging.info(f"All user IDs in {fact_table} match with {dim_table}.")
    except Exception as e:
        logging.error(f"Error during user id check in {dim_table}: {e}")

def check_event_type_consistency(fact_df, dim_df, fact_table, dim_table):
    """Checks if all event types in the fact table exist in the dimension table."""
    try:
        unmatched_event_types = fact_df[~fact_df['event_name'].isin(dim_df['event_type_name'])]
        if len(unmatched_event_types) > 0:
            logging.warning(f"Unmatched event types found in {fact_table} with {dim_table}.")
        else:
            logging.info(f"All event types in {fact_table} match with {dim_table}.")
    except Exception as e:
        logging.error(f"Error during event type check in {dim_table}: {e}")

def check_currency_consistency(fact_df, dim_currency, fact_name, dim_name):
    """Checks if all currencies in the fact table exist in the currency dimension table."""
    try:
        invalid_currencies = fact_df[~fact_df['currency'].isin(dim_currency['currency_name'])]
        if not invalid_currencies.empty:
            logging.warning(f"Found {len(invalid_currencies)} invalid currencies in {fact_name} not found in {dim_name}.")
        else:
            logging.info(f"Currency consistency check passed between {fact_name} and {dim_name}.")
    except Exception as e:
        logging.error(f"Error during currency consistency check: {e}")

def check_date_range(df, column, min_date, max_date, table_name):
    """Checks if the date column values fall within the specified range."""
    try:
        # Convert the column to datetime if not already
        df[column] = pd.to_datetime(df[column], errors='coerce')
        
        min_date = pd.to_datetime(min_date)
        max_date = pd.to_datetime(max_date)
        
        out_of_range = df[(df[column] < min_date) | (df[column] > max_date)]
        if not out_of_range.empty:
            logging.warning(f"{len(out_of_range)} rows in {table_name} have {column} values outside the range {min_date} to {max_date}.")
        else:
            logging.info(f"All dates in {column} of {table_name} are within the specified range.")
    except Exception as e:
        logging.error(f"Error during date range check in {table_name}: {e}")

def check_interface_consistency(fact_df, dim_interface, fact_name, dim_name):
    """Checks if all interfaces in the fact table exist in the interface dimension table."""
    try:
        invalid_interfaces = fact_df[~fact_df['interface'].isin(dim_interface['interface_name'])]
        if not invalid_interfaces.empty:
            logging.warning(f"Found {len(invalid_interfaces)} invalid interfaces in {fact_name} not found in {dim_name}.")
        else:
            logging.info(f"Interface consistency check passed between {fact_name} and {dim_name}.")
    except Exception as e:
        logging.error(f"Error during interface consistency check: {e}")



