import pandas as pd
import json
from pathlib import Path
import os
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB connection
MONGO_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("MONGODB_DB_NAME", "data-visualizer")

if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is not set")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
monthly_collection = db["tourism_monthly"]
country_collection = db["tourism_country"]

def clean_dataframe(df):
    # Skip the header rows and reset index
    df = df.iloc[2:].reset_index(drop=True)
    
    # Rename columns
    df.columns = ['No', 'Country', 'January', 'February', 'March', 'April', 'May', 'June',
                 'July', 'August', 'September', 'October', 'November', 'December', 'Total']
    
    # Convert numeric columns to float, handling any non-numeric values
    numeric_columns = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December', 'Total']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                          np.int16, np.int32, np.int64, np.uint8,
                          np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        return super().default(obj)

def process_tourism_data(excel_file):
    # Read the Excel file
    df = pd.read_excel(excel_file)
    df = clean_dataframe(df)
    
    # Get the year from the filename
    year = int(Path(excel_file).stem.split('_')[2])
    
    # List of months
    months = ['January', 'February', 'March', 'April', 'May', 'June', 
              'July', 'August', 'September', 'October', 'November', 'December']
    
    # Find the total row (where No column contains 'Total')
    total_row = df[df['No'].astype(str).str.contains('Total', case=False, na=False)].iloc[0]
    
    # Process monthly data
    monthly_data = []
    for month in months:
        # Calculate total by summing all country values
        calculated_total = 0
        for _, row in df.iterrows():
            if pd.notna(row['No']) and str(row['No']).lower() != 'total' and pd.notna(row[month]):
                calculated_total += float(row[month])
        
        # Get total from the total row
        total_from_row = float(total_row[month]) if pd.notna(total_row[month]) else 0
        
        month_data = {
            'year': year,
            'Month': month,
            'total': total_from_row,  # Main total from row
            'calculated_total': calculated_total  # Additional total from calculation
        }
        # Add data for each country
        for _, row in df.iterrows():
            country = str(row['Country']).strip()
            # Skip the total row and empty countries
            if country and pd.notna(row['No']) and str(row['No']).lower() != 'total':
                if pd.notna(row[month]):
                    month_data[country] = float(row[month])
        monthly_data.append(month_data)
    
    # Process country-wise data
    country_data = []
    for _, row in df.iterrows():
        country = str(row['Country']).strip()
        # Skip the total row and empty countries
        if not country or (pd.notna(row['No']) and str(row['No']).lower() == 'total'):
            continue
            
        # Get total from 'Total' column
        total_from_column = float(row['Total']) if pd.notna(row['Total']) else 0
        
        # Calculate total by summing monthly values
        calculated_total = float(sum(row[month] for month in months if pd.notna(row[month])))
        
        country_info = {
            'Year': year,
            'Country': country,
            'total': total_from_column,  # Total from 'Total' column
            'calculated_total': calculated_total  # Total calculated from monthly values
        }
        # Add monthly data
        for month in months:
            if pd.notna(row[month]):
                country_info[month] = float(row[month])
        country_data.append(country_info)
    
    return monthly_data, country_data

def save_to_mongodb(monthly_data, country_data, year):
    try:
        # Delete existing data for the year
        monthly_collection.delete_many({'year': year})
        country_collection.delete_many({'Year': year})
        
        # Insert new data
        if monthly_data:
            monthly_collection.insert_many(monthly_data)
        if country_data:
            country_collection.insert_many(country_data)
            
        print(f"Successfully saved data for {year} to MongoDB")
    except Exception as e:
        print(f"Error saving to MongoDB: {str(e)}")

def main():
    # Process all Excel files in the tourism directory
    tourism_dir = Path('data/tourism')
    for excel_file in tourism_dir.glob('*.xlsx'):
        print(f"Processing {excel_file}")
        
        # Get the year from filename
        year = int(Path(excel_file).stem.split('_')[2])
        
        # Process the data
        monthly_data, country_data = process_tourism_data(excel_file)
        
        # Save to MongoDB
        save_to_mongodb(monthly_data, country_data, year)
        
        print(f"Completed processing data for {year}")

if __name__ == "__main__":
    main()
