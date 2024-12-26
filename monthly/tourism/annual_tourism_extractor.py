from pymongo import MongoClient
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_mongodb():
    try:
        mongodb_uri = os.getenv('MONGODB_URI')
        db_name = os.getenv('MONGODB_DB_NAME')
        client = MongoClient(mongodb_uri)
        db = client[db_name]
        return db
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        raise

def create_annual_tourism_data():
    try:
        db = connect_to_mongodb()
        monthly_collection = db['tourism_monthly']
        annual_collection = db['tourism_annually']

        # Get all unique years from monthly collection
        years = monthly_collection.distinct('year')
        
        # Process each year
        for year in years:
            # Get all monthly documents for this year
            monthly_docs = list(monthly_collection.find({'year': year}))
            
            # Initialize data
            annual_total = 0
            update_data = {'year': year}
            
            # Process each month's data
            for doc in monthly_docs:
                month = doc.get('month', '').lower()
                total = doc.get('total', 0)
                update_data[month] = total
                annual_total += total
            
            # Add total to update data
            update_data['total'] = annual_total
            
            # Create or update annual document
            annual_collection.update_one(
                {'year': year},
                {'$set': update_data},
                upsert=True
            )
            
            logger.info(f"Processed year {year} with total: {annual_total}")
            logger.info(f"Monthly breakdown for {year}: {update_data}")
            
    except Exception as e:
        logger.error(f"Error processing annual tourism data: {e}")
        raise

if __name__ == "__main__":
    try:
        create_annual_tourism_data()
        logger.info("Annual tourism data processing completed successfully")
    except Exception as e:
        logger.error(f"Failed to process annual tourism data: {e}")
