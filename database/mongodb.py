import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Reset any existing env variables
os.environ.pop('MONGODB_URI', None)
os.environ.pop('MONGODB_DB_NAME', None)

# Load environment variables from root directory
root_dir = Path(__file__).resolve().parents[1]  # Go up 1 level to reach root
env_path = root_dir / '.env'
print(f"Loading .env from: {env_path}")
print(f"File exists: {env_path.exists()}")

# Load the .env file
load_dotenv(env_path, override=True)

# Print environment variables
mongodb_uri = os.getenv('MONGODB_URI')
db_name = os.getenv('MONGODB_DB_NAME', 'data-visualizer')
# print(f"MONGODB_URI: {mongodb_uri}")
# print(f"MONGODB_DB_NAME: {db_name}")

def get_database():
    """
    Get MongoDB database connection
    Returns:
        pymongo.database.Database: MongoDB database instance
    """
    try:
        if not mongodb_uri:
            raise ValueError("MONGODB_URI environment variable is not set")
            
        client = MongoClient(mongodb_uri)
        db = client[db_name]
        return db
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        raise
