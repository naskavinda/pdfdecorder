import pandas as pd
import os
from datetime import datetime
from pathlib import Path
import logging
import sys

# Add root directory to Python path
root_dir = Path(__file__).resolve().parents[2]
root_dir_str = str(root_dir)
if root_dir_str not in sys.path:
    sys.path.append(root_dir_str)

from database.mongodb import get_database

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_excel_data(file_path):
    try:
        # Read the Excel file
        df = pd.read_excel(file_path)
        print("Columns in Excel:", df.columns.tolist())
        
        # Convert the dataframe to the required format
        yearly_data = []
        
        for _, row in df.iterrows():
            try:
                data = {
                    "year": int(row.iloc[0]),  # Assuming first column is Year
                    "january": int(row.iloc[1]),
                    "february": int(row.iloc[2]),
                    "march": int(row.iloc[3]),
                    "april": int(row.iloc[4]),
                    "may": int(row.iloc[5]),
                    "june": int(row.iloc[6]),
                    "july": int(row.iloc[7]),
                    "august": int(row.iloc[8]),
                    "september": int(row.iloc[9]),
                    "october": int(row.iloc[10]),
                    "november": int(row.iloc[11]),
                    "december": int(row.iloc[12]),
                    "total": int(row.iloc[13])
                }
                yearly_data.append(data)
            except Exception as e:
                print(f"Error processing row: {row}")
                print(f"Error details: {str(e)}")
                continue
        
        return yearly_data
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return []

def main():
    try:
        # Process Excel data
        excel_path = Path("data/tourism/Yearly_data.xlsx")
        yearly_data = process_excel_data(excel_path)
        
        # Connect to MongoDB
        db = get_database()
        collection = db['tourism_annually']
        
        # Insert data into MongoDB
        if yearly_data:
            # First, remove existing data to avoid duplicates
            collection.delete_many({})
            
            # Insert new data
            result = collection.insert_many(yearly_data)
            print(f"Successfully inserted {len(result.inserted_ids)} documents")
        else:
            print("No data to insert")
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
