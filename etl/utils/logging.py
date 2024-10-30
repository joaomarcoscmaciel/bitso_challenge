import logging

def setup_logging(log_file):
    """Sets up logging configuration."""
    logging.basicConfig(
        filename=log_file,                   # Log file path
        filemode='a',                        # Append mode
        level=logging.INFO,                  # Log level (INFO, WARNING, ERROR, etc.)
        format='%(asctime)s - %(levelname)s - %(message)s'  # Log format
    )

    # Add console output for debugging
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger('').addHandler(console)
