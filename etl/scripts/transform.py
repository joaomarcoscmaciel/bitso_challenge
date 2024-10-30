import pandas as pd
import logging

def transform_fact_withdrawals(withdrawals_df):
    """Transforms withdrawals data into the Fact_Withdrawals table."""
    try:
        logging.info("Starting transformation for Fact_Withdrawals...")
        # Convert event_timestamp to datetime
        withdrawals_df['event_timestamp'] = pd.to_datetime(withdrawals_df['event_timestamp'], errors='coerce')
        withdrawals_df['event_timestamp'] = withdrawals_df['event_timestamp'].dt.tz_localize(None)

        # Drop rows with missing critical fields
        withdrawals_df.dropna(subset=['user_id', 'event_timestamp', 'amount'], inplace=True)

        # Convert amount to numeric and handle missing or negative values
        withdrawals_df['amount'] = pd.to_numeric(withdrawals_df['amount'], errors='coerce')
        withdrawals_df = withdrawals_df[withdrawals_df['amount'] > 0]  # Remove rows with zero or negative amounts

        # Filter outliers by setting upper limit for amount (e.g., top 99th percentile)
        upper_limit = withdrawals_df['amount'].quantile(0.99)
        withdrawals_df = withdrawals_df[withdrawals_df['amount'] <= upper_limit]

        # Select necessary columns for Fact_Withdrawals
        fact_withdrawals_table = withdrawals_df[['id', 'event_timestamp', 'user_id', 'amount', 'interface', 'currency', 'tx_status']]

        return fact_withdrawals_table
    except Exception as e:
        logging.error(f"Error transforming Fact_Withdrawals: {e}")
        raise

def transform_fact_deposits(deposits_df):
    """Transforms deposits data into the Fact_Deposits table with data quality checks."""
    try:
        logging.info("Starting transformation for Fact_Deposits...")

        # Convert event_timestamp to datetime and handle missing values
        deposits_df['event_timestamp'] = pd.to_datetime(deposits_df['event_timestamp'], errors='coerce')
        deposits_df['event_timestamp'] = deposits_df['event_timestamp'].dt.tz_localize(None)

        # Remove rows with missing user_id or event_timestamp
        deposits_df.dropna(subset=['user_id', 'event_timestamp'], inplace=True)

        # Convert amount to numeric and handle missing or negative values
        deposits_df['amount'] = pd.to_numeric(deposits_df['amount'], errors='coerce')
        deposits_df = deposits_df[deposits_df['amount'] > 0]  # Remove rows with zero or negative amounts

        # Filter only completed transactions
        deposits_df = deposits_df[deposits_df['tx_status'] == 'complete']

        # Remove duplicates based on ID
        deposits_df.drop_duplicates(subset=['id'], inplace=True)

        # Filter amounts within a reasonable range (e.g., up to 1 billion)
        upper_limit = 1e9
        deposits_df = deposits_df[deposits_df['amount'] <= upper_limit]

        # Select necessary columns for Fact_Deposits
        fact_deposits_table = deposits_df[['id', 'event_timestamp', 'user_id', 'amount', 'currency', 'tx_status']]

        logging.info("Fact_Deposits transformation completed.")
        return fact_deposits_table
    except Exception as e:
        logging.error(f"Error transforming Fact_Deposits: {e}")
        raise


def transform_fact_events(events_df):
    """Transforms events data into the Fact_Events table."""
    try:
        logging.info("Starting transformation for Fact_Events...")
        # Convert event_timestamp to datetime
        events_df['event_timestamp'] = pd.to_datetime(events_df['event_timestamp'], errors='coerce')
        events_df['event_timestamp'] = events_df['event_timestamp'].dt.tz_localize(None)

        # Drop rows with missing critical fields
        events_df.dropna(subset=['user_id', 'event_timestamp', 'event_name'], inplace=True)

        # Select necessary columns for Fact_Events
        fact_events_table = events_df[['id', 'event_timestamp', 'user_id', 'event_name']]

        return fact_events_table
    except Exception as e:
        logging.error(f"Error transforming Fact_Events: {e}")
        raise


def transform_user_data(user_id_df, user_level_df):
    """Transforms user data into the Dim_User table."""
    try:
        logging.info("Starting transformation for Dim_User...")
        user_level_df['event_timestamp'] = pd.to_datetime(user_level_df['event_timestamp'], errors='coerce')
        user_level_df['event_timestamp'] = user_level_df['event_timestamp'].dt.tz_localize(None)
        
        latest_user_level = user_level_df.sort_values(by='event_timestamp').groupby('user_id').tail(1)
        dim_user = pd.merge(user_id_df, latest_user_level, on='user_id', how='left')
        dim_user_table = dim_user[['user_id', 'jurisdiction', 'level']].drop_duplicates()
        
        return dim_user_table
    except Exception as e:
        logging.error(f"Error transforming Dim_User: {e}")
        raise


def transform_dim_currency(fact_withdrawals_df, fact_deposits_df):
    """Creates the Dim_Currency table from Fact_Withdrawals and Fact_Deposits."""
    try:
        logging.info("Starting transformation for Dim_Currency...")

        # Combine currencies from both fact tables
        combined_currencies = pd.concat([fact_withdrawals_df[['currency']], fact_deposits_df[['currency']]])
        
        # Extract unique currencies
        unique_currencies = combined_currencies.drop_duplicates().reset_index(drop=True)
        unique_currencies['currency_id'] = unique_currencies.index + 1

        # Rename column for clarity
        dim_currency = unique_currencies.rename(columns={'currency': 'currency_name'})

        return dim_currency
    except Exception as e:
        logging.error(f"Error transforming Dim_Currency: {e}")
        raise


def transform_dim_interface(fact_withdrawals_df, fact_deposits_df):
    """Creates the Dim_Interface table from Fact_Withdrawals and Fact_Deposits."""
    try:
        logging.info("Starting transformation for Dim_Interface...")

        # Extract unique interfaces
        unique_interfaces = fact_withdrawals_df[['interface']].drop_duplicates().reset_index(drop=True)
        unique_interfaces['interface_id'] = unique_interfaces.index + 1

        # Rename column for clarity
        dim_interface = unique_interfaces.rename(columns={'interface': 'interface_name'})

        logging.info("Dim_Interface transformation completed.")
        return dim_interface
    except Exception as e:
        logging.error(f"Error transforming Dim_Interface: {e}")
        raise


def transform_dim_time(min_date, max_date):
    """Creates the Dim_Time table based on the date range from min_date to max_date."""
    try:
        logging.info("Starting transformation for Dim_Time...")
        # Generate date range
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')

        # Create a DataFrame for Dim_Time
        dim_time = pd.DataFrame(date_range, columns=['date'])
        dim_time['year'] = dim_time['date'].dt.year
        dim_time['month'] = dim_time['date'].dt.month
        dim_time['day'] = dim_time['date'].dt.day
        dim_time['quarter'] = dim_time['date'].dt.quarter
        dim_time['day_of_week'] = dim_time['date'].dt.dayofweek
        dim_time['is_weekend'] = dim_time['date'].dt.dayofweek >= 5
        dim_time.reset_index(drop=True, inplace=True)
        dim_time['time_id'] = dim_time.index + 1

        return dim_time
    except Exception as e:
        logging.error(f"Error transforming Dim_Time: {e}")
        raise


def transform_dim_event_type(fact_events_df):
    """Creates the Dim_Event_Type table from Fact_Events."""
    try:
        logging.info("Starting transformation for Dim_Event_Type...")
        # Extract unique event types
        unique_event_types = fact_events_df[['event_name']].drop_duplicates()
        unique_event_types.reset_index(drop=True, inplace=True)
        unique_event_types['event_type_id'] = unique_event_types.index + 1

        # Rename column for clarity
        dim_event_type = unique_event_types.rename(columns={'event_name': 'event_type_name'})

        return dim_event_type
    except Exception as e:
        logging.error(f"Error transforming Dim_Event_Type: {e}")
        raise


