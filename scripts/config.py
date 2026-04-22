import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'aact'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password'),
}

# ISRCTN Database configuration (Separate from AACT)
ISRCTN_DB_CONFIG = {
    'host': os.getenv('ISRCTN_DB_HOST', os.getenv('DB_HOST', 'localhost')),
    'port': int(os.getenv('ISRCTN_DB_PORT', os.getenv('DB_PORT', 5432))),
    'database': os.getenv('ISRCTN_DB_NAME', 'isrctn_repository'),
    'user': os.getenv('ISRCTN_DB_USER', os.getenv('DB_USER', 'postgres')),
    'password': os.getenv('ISRCTN_DB_PASSWORD', os.getenv('DB_PASSWORD', 'password')),
}

# Schema configuration
SCHEMA = os.getenv('DB_SCHEMA', 'ctgov')
