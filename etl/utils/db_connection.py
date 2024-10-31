import yaml
from sqlalchemy import create_engine

def load_config(file_path='config/config.yml'):
    """Loads the configuration from a YAML file."""
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        raise RuntimeError(f"Error loading config file: {e}")

def get_postgres_engine(config_path='config/config.yml'):
    """Creates a PostgreSQL engine based on the config file."""
    try:
        config = load_config(config_path)
        db_config = config.get('database', {})

        db_user = db_config.get('user', 'admin')
        db_password = db_config.get('password', 'password')
        db_host = db_config.get('host', 'localhost')
        db_port = db_config.get('port', 5432)
        db_name = db_config.get('name', 'etl_db')

        # Create the PostgreSQL connection URL
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        # Create and return the SQLAlchemy engine
        return create_engine(db_url)
    except Exception as e:
        raise RuntimeError(f"Error creating PostgreSQL engine: {e}")

