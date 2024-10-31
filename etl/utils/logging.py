import logging

def setup_logging(log_file):
    """Sets up logging configuration."""
    logging.basicConfig(
        filename=log_file,                
        filemode='a',                       
        level=logging.INFO,                 
        format='%(asctime)s - %(levelname)s - %(message)s'  
    )

    # Add console output for debugging
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger('').addHandler(console)
