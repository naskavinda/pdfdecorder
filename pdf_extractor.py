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
            # Convert cell to string and remove extra spaces
            price_str = str(cell).strip()
            
            # Return empty if n.a.
            if price_str == 'n.a.':
                return '', ''
                
            # Handle multiple prices separated by newlines
            if '\n' in price_str:
                prices = price_str.split('\n')
                if len(prices) >= 2:
                    # Return both prices
                    price1 = prices[0].replace(' ', '')
                    price2 = prices[1].replace(' ', '')
                    return price1, price2
            
            # Remove spaces but keep commas for now
            price_str = price_str.replace(' ', '')
            
            # Return the price as is, with commas preserved
            return price_str, price_str
            
    except Exception as e:
        print(f"Error processing price: {str(e)}")
    return '', ''

def find_section_boundaries(table, header_row_idx):
    """Find the start and end indices for a section in the table"""
    start_idx = None
    end_idx = None
    
    # Search for the VEGETABLES section header
    for i in range(header_row_idx, len(table)):
        row = table[i]
        if not row:
            continue
            
        # Join the row elements to check for section header
        row_text = ' '.join(str(cell) for cell in row if cell)
        if 'V E G' in row_text:
            start_idx = i + 2  # Skip the empty row after header
            break
    
    if start_idx is None:
        print("Could not find VEGETABLES section start")
        return None, None
        
    # Search for the end of section (next section header or empty rows)
    section_markers = ['O T H E R', 'F R U I T S', 'R I C E', 'F I S H']
    empty_row_count = 0
    
    for i in range(start_idx, len(table)):
        row = table[i]
        if not row or all(not cell for cell in row):
            empty_row_count += 1
            if empty_row_count >= 3:  # Three consecutive empty rows
                end_idx = i - 2  # Go back to before empty rows
                break
        else:
            empty_row_count = 0
            
        # Check for next section header
        row_text = ' '.join(str(cell) for cell in row if cell)
        for marker in section_markers:
            if marker in row_text:
                end_idx = i
                break
                
        if end_idx:
            break
            
    # If no clear end found, use a default value
    if not end_idx:
        print("Could not find section end")
        # Set end index to start + 20 rows or table length, whichever is smaller
        end_idx = min(start_idx + 20, len(table))
        print(f"Using default end index: {end_idx}")
    
    return start_idx, end_idx

def clean_price(price_str):
    """
    Clean and convert price string to float.
    """
    try:
        if not price_str or price_str == 'n.a.':
            return None
            
        # Remove spaces
        price_str = price_str.replace(' ', '')
        
        # If the price starts with a comma, add a leading digit
        if price_str.startswith(','):
            price_str = '1' + price_str
            
        # Now convert to float, after removing the comma
        return float(price_str.replace(',', ''))
    except Exception as e:
        print(f"Error cleaning price {price_str}: {str(e)}")
        return None

def extract_prices(row):
    """
    Extract and clean price values from a row.
    Returns tuple of (pettah_wholesale_yesterday, pettah_wholesale_today,
                     dambulla_wholesale_yesterday, dambulla_wholesale_today,
                     pettah_retail_yesterday, pettah_retail_today,
                     dambulla_retail_yesterday, dambulla_retail_today,
                     narahenpita_retail_yesterday, narahenpita_retail_today)
    """
    try:
        # Print row contents for debugging
        print(f"Raw row data: {row}")
        print(f"Row length: {len(row)}")
        if len(row) > 16:
            print(f"Narahenpita yesterday (index 16): {row[16]}")
        if len(row) > 18:
            print(f"Narahenpita today (index 18): {row[18]}")
            
        # Extract Pettah wholesale prices (columns 3 and 5)
        pettah_wholesale_y = clean_price(row[3]) if len(row) > 3 else None
        pettah_wholesale_t = clean_price(row[5]) if len(row) > 5 else None
        
        # Extract Dambulla wholesale prices (columns 6 and 8)
        dambulla_wholesale_y = clean_price(row[6]) if len(row) > 6 else None
        dambulla_wholesale_t = clean_price(row[8]) if len(row) > 8 else None
        
        # Extract Pettah retail prices (columns 9 and 10)
        pettah_retail_y = clean_price(row[9]) if len(row) > 9 else None
        pettah_retail_t = clean_price(row[10]) if len(row) > 10 else None
        
        # Extract Dambulla retail prices (columns 12 and 14)
        dambulla_retail_y = clean_price(row[12]) if len(row) > 12 else None
        dambulla_retail_t = clean_price(row[14]) if len(row) > 14 else None
        
        # Extract Narahenpita retail prices (columns 16 and 18)
        narahenpita_retail_y = clean_price(row[16]) if len(row) > 16 else None
        narahenpita_retail_t = clean_price(row[18]) if len(row) > 18 else None
        
        # Print extracted Narahenpita prices for debugging
        print(f"Extracted Narahenpita prices - Yesterday: {narahenpita_retail_y}, Today: {narahenpita_retail_t}")
        
        return (pettah_wholesale_y, pettah_wholesale_t,
                dambulla_wholesale_y, dambulla_wholesale_t,
                pettah_retail_y, pettah_retail_t,
                dambulla_retail_y, dambulla_retail_t,
                narahenpita_retail_y, narahenpita_retail_t)
                
    except Exception as e:
        print(f"Error extracting prices: {str(e)}")
        return None, None, None, None, None, None, None, None, None, None

def process_table_data(table):
    """
    Convert table data to a MongoDB-compatible format with proper column mapping
    """
    # Find header row
    header_row_idx = None
    for i, row in enumerate(table):
        if row and len(row) > 0 and row[0] == 'Item':
            header_row_idx = i
            print(f"Found header row at index {i}")
            break
    
    if header_row_idx is None:
        print("Could not find header row")
        return None
    
    # Find vegetables section
    veg_start_idx, veg_end_idx = find_section_boundaries(table, header_row_idx)
    if veg_start_idx is None:
        print("Could not find VEGETABLES section")
        return None
    
    print(f"\nProcessing vegetable rows from index {veg_start_idx} to {veg_end_idx}")
    
    # Process each row in the vegetables section
    processed_rows = []
    row_num = 0
    
    for i in range(veg_start_idx, veg_end_idx):
        row = table[i]
        if not row or len(row) < 2:  # Skip empty rows
            print(f"Skipping row {row_num} due to insufficient data")
            row_num += 1
            continue
            
        item_name = row[0].strip() if row[0] else None
        if not item_name:
            print(f"Skipping row {row_num} due to invalid item name: {item_name}")
            row_num += 1
            continue
            
        unit = row[1].strip() if len(row) > 1 else None
        
        # Extract all prices using the new function
        (pettah_wholesale_yesterday, pettah_wholesale_today,
         dambulla_wholesale_yesterday, dambulla_wholesale_today,
         pettah_retail_yesterday, pettah_retail_today,
         dambulla_retail_yesterday, dambulla_retail_today,
         narahenpita_retail_yesterday, narahenpita_retail_today) = extract_prices(row)
        
        print(f"\nProcessing item: {item_name}")
        print(f"Unit: {unit}")
        print(f"Pettah wholesale: {pettah_wholesale_yesterday} / {pettah_wholesale_today}")
        print(f"Pettah retail: {pettah_retail_yesterday} / {pettah_retail_today}")
        print(f"Dambulla wholesale: {dambulla_wholesale_yesterday} / {dambulla_wholesale_today}")
        print(f"Dambulla retail: {dambulla_retail_yesterday} / {dambulla_retail_today}")
        print(f"Narahenpita retail: {narahenpita_retail_yesterday} / {narahenpita_retail_today}")
        
        row_data = {
            'item': item_name,
            'unit': unit,
            'pettah_wholesale': {
                'yesterday': pettah_wholesale_yesterday,
                'today': pettah_wholesale_today
            },
            'dambulla_wholesale': {
                'yesterday': dambulla_wholesale_yesterday,
                'today': dambulla_wholesale_today
            },
            'pettah_retail': {
                'yesterday': pettah_retail_yesterday,
                'today': pettah_retail_today
            },
            'dambulla_retail': {
                'yesterday': dambulla_retail_yesterday,
                'today': dambulla_retail_today
            },
            'narahenpita_retail': {
                'yesterday': narahenpita_retail_yesterday,
                'today': narahenpita_retail_today
            }
        }
        
        processed_rows.append(row_data)
        print(f"Added row for item: {item_name}")
        row_num += 1
        
    print(f"\nTotal rows processed: {len(processed_rows)}")
    return processed_rows

def extract_pdf_data(pdf_path):
    """
    Extract tables from PDF using pdfplumber and return the data
    """
    try:
        print(f"Reading page 2 from {pdf_path}...")
        with pdfplumber.open(pdf_path) as pdf:
            # Get page 2 (0-based index)
            page = pdf.pages[1]
            
            # Extract table with specific settings
            table = page.extract_table({
                'vertical_strategy': 'text',
                'horizontal_strategy': 'text',
                'intersection_y_tolerance': 10,
                'intersection_x_tolerance': 10,
                'snap_y_tolerance': 3,
                'snap_x_tolerance': 3,
                'join_tolerance': 3,
                'edge_min_length': 3,
                'min_words_vertical': 3,
                'min_words_horizontal': 1
            })
            
            if not table:
                print(f"No table found on page 2 of {pdf_path}")
                return None
            
            # Print raw table data for debugging
            print("\nRaw table data:")
            for i, row in enumerate(table):
                print(f"Row {i}: {row}")
            
            # Get the date from filename (assuming format YYYY-MM-DD.pdf)
            date_str = os.path.basename(pdf_path).replace('.pdf', '')
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            
            # Process the table data
            processed_data = process_table_data(table)
            
            # Create document for MongoDB
            document = {
                'date': date_obj,
                'page': 2,
                'table_index': 0,
                'data': processed_data
            }
            
            return [document]
            
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
