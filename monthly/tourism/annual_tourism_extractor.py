import logging
import os
from pathlib import Path
import sys

# Add root directory to Python path
root_dir = Path(__file__).resolve().parents[2]
root_dir_str = str(root_dir)
if root_dir_str not in sys.path:
    sys.path.append(root_dir_str)

from database.mongodb import get_database

def create_annual_tourism_data():
    try:
        db = get_database()
        monthly_collection = db['tourism_monthly']
        annual_collection = db['tourism_annually']

        # Get all unique years from monthly collection
        years = monthly_collection.distinct('year')

        # Expected months (all lowercase)
        expected_months = ['january', 'february', 'march', 'april', 'may', 'june', 
                         'july', 'august', 'september', 'october', 'november', 'december']

        # Process each year
        for year in years:
            # Check if year exists in annual collection
            existing_data = annual_collection.find_one({'year': year})

            if existing_data:
                # Check if any month is missing
                missing_months = [month for month in expected_months if month not in existing_data]
                if not missing_months:
                    print(f"Year {year} has complete data. Skipping.")
                    continue

                print(f"Found missing months for year {year}: {missing_months}. Reprocessing entire year.")

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

            print(f"Processed year {year} with total: {annual_total}")
            print(f"Monthly breakdown for {year}: {update_data}")

    except Exception as e:
        print(f"Error processing annual tourism data: {e}")
        raise

if __name__ == "__main__":
    try:
        create_annual_tourism_data()
        print("Annual tourism data processing completed successfully")
    except Exception as e:
        print(f"Failed to process annual tourism data: {e}")
