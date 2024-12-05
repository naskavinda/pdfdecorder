import os
import pdfplumber
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# MongoDB connection
client = MongoClient('mongodb://root:secret@localhost:27017/')
db = client['pdf_data']
collection = db['extracted_tables']

def safe_get_price(cell):
    """Safely get price from cell"""
    try:
        if cell and not pd.isna(cell):
            price_str = str(cell).strip()
            
            # Handle multiple prices separated by newlines
            if '\n' in price_str:
                prices = price_str.split('\n')
                if prices:
                    # Take the first price for the current item
                    price_str = prices[0]
            
            # Remove commas and spaces
            price_str = price_str.replace(',', '').replace(' ', '')
            
            # Try to split by \r first
            if '\\r' in price_str:
                prices = price_str.split('\\r')
            else:
                # If no \r, try to split in the middle
                if len(price_str) >= 6:  # Two 3-digit numbers minimum
                    mid = len(price_str) // 2
                    prices = [price_str[:mid], price_str[mid:]]
                else:
                    prices = [price_str]
            
            # Clean up prices
            prices = [p.strip() for p in prices if p and p.strip() and p.strip() != 'n.a']
            if len(prices) >= 2:
                return prices[0], prices[1]
            elif len(prices) == 1:
                return prices[0], prices[0]
    except Exception as e:
        print(f"Error processing price: {str(e)}")
    return '', ''

def find_vegetable_section(table):
    """Find the start and end of the vegetable section"""
    start_idx = None
    end_idx = None
    
    for idx, row in enumerate(table):
        if row and any(cell and 'V E G E T A B L E S' in str(cell) for cell in row):
            start_idx = idx + 1
        elif start_idx is not None and row and any(cell and 'O T H E R' in str(cell) for cell in row):
            end_idx = idx
            break
    
    return start_idx, end_idx

def process_table_data(table):
    """
    Convert table data to a MongoDB-compatible format with proper column mapping
    """
    # Find the vegetable section
    start_idx, end_idx = find_vegetable_section(table)
    if start_idx is None or end_idx is None:
        print("Could not find vegetable section")
        return {'rows': []}
    
    # Get the vegetable rows
    vegetable_rows = table[start_idx:end_idx]
    
    # Process rows
    rows = []
    items = ['Beans', 'Carrot', 'Leeks', 'Cabbage', 'Tomato', 'Brinjal', 'Ladies Fingers', 'Pumpkin', 'Ash Plantain']
    
    # Get Dambulla prices from the combined cell
    dambulla_wholesale_prices = []
    dambulla_retail_prices = []
    
    # Find and parse Dambulla prices from the first row
    if vegetable_rows and len(vegetable_rows[0]) >= 9:
        dambulla_wholesale_str = str(vegetable_rows[0][4]) if vegetable_rows[0][4] else ''
        dambulla_retail_str = str(vegetable_rows[0][8]) if vegetable_rows[0][8] else ''
        
        if dambulla_wholesale_str:
            dambulla_wholesale_prices = [p.strip() for p in dambulla_wholesale_str.split('\n') if p.strip()]
        if dambulla_retail_str:
            dambulla_retail_prices = [p.strip() for p in dambulla_retail_str.split('\n') if p.strip()]
    
    for idx, item in enumerate(items):
        if idx >= len(vegetable_rows):
            break
            
        row = vegetable_rows[idx]
        if not row or len(row) < 11:  # Make sure we have enough columns
            continue
        
        # Get Pettah prices
        pettah_wholesale_y, pettah_wholesale_t = safe_get_price(row[2])
        pettah_retail_y, pettah_retail_t = safe_get_price(row[6])
        
        # Get Dambulla prices for this item
        dambulla_wholesale_y, dambulla_wholesale_t = '', ''
        dambulla_retail_y, dambulla_retail_t = '', ''
        
        if idx < len(dambulla_wholesale_prices):
            dambulla_wholesale_y, dambulla_wholesale_t = safe_get_price(dambulla_wholesale_prices[idx])
        if idx < len(dambulla_retail_prices):
            dambulla_retail_y, dambulla_retail_t = safe_get_price(dambulla_retail_prices[idx])
        
        # Get Narahenpita prices
        narahenpita_retail_y, narahenpita_retail_t = safe_get_price(row[10])
        
        # Create a row dictionary with all prices
        row_dict = {
            'item': item,
            'unit': 'Rs./kg',
            'wholesale': {
                'pettah': {
                    'yesterday': pettah_wholesale_y,
                    'today': pettah_wholesale_t
                },
                'dambulla': {
                    'yesterday': dambulla_wholesale_y,
                    'today': dambulla_wholesale_t
                }
            },
            'retail': {
                'pettah': {
                    'yesterday': pettah_retail_y,
                    'today': pettah_retail_t
                },
                'dambulla': {
                    'yesterday': dambulla_retail_y,
                    'today': dambulla_retail_t
                },
                'narahenpita': {
                    'yesterday': narahenpita_retail_y,
                    'today': narahenpita_retail_t
                }
            }
        }
        rows.append(row_dict)
    
    return {
        'rows': rows
    }

def extract_pdf_data(pdf_path):
    """
    Extract tables from PDF using pdfplumber and return the data
    """
    try:
        print(f"Reading page 2 from {pdf_path}...")
        with pdfplumber.open(pdf_path) as pdf:
            # Get page 2 (0-based index)
            page = pdf.pages[1]
            
            # Extract tables from the page
            tables = page.extract_tables()
            
            if not tables:
                print(f"No tables found on page 2 of {pdf_path}")
                return None
            
            # Get the date from filename (assuming format YYYY-MM-DD.pdf)
            date_str = os.path.basename(pdf_path).replace('.pdf', '')
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            
            # Process each table
            extracted_data = []
            merged_data = {'rows': []}
            
            # Find the main price table (should have 'V E G E T A B L E S' section)
            for table in tables:
                if not table:
                    continue
                
                # Check if this is the main price table
                for row in table:
                    if row and any(cell and 'V E G E T A B L E S' in str(cell) for cell in row):
                        # Process the table data
                        processed_data = process_table_data(table)
                        
                        # Merge data from all tables
                        merged_data['rows'].extend(processed_data['rows'])
                        break
            
            # Create document for MongoDB
            document = {
                'date': date_obj,
                'page': 2,
                'table_index': 0,
                'data': merged_data
            }
            
            extracted_data.append(document)
            
            return extracted_data
            
    except Exception as e:
        print(f"Error extracting data from {pdf_path}: {str(e)}")
        return None

def main():
    # Create reports directory if it doesn't exist
    os.makedirs('reports', exist_ok=True)
    
    # Process all PDF files in the data directory
    pdf_dir = 'data'
    for filename in os.listdir(pdf_dir):
        if filename.endswith('.pdf'):
            # Extract data from PDF
            pdf_path = os.path.join(pdf_dir, filename)
            extracted_data = extract_pdf_data(pdf_path)
            
            if extracted_data:
                # Store in MongoDB
                for document in extracted_data:
                    # Use date and table_index as unique identifier
                    query = {
                        'date': document['date'],
                        'table_index': document['table_index']
                    }
                    
                    # Update or insert the document
                    collection.update_one(
                        query,
                        {'$set': document},
                        upsert=True
                    )
                    
                print(f"Successfully processed and stored data from {filename}")
            else:
                print(f"Failed to process {filename}")

if __name__ == "__main__":
    main()
