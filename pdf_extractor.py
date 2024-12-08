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

def find_other_section_boundaries(table, header_row_idx):
    """Find the start and end indices for the Other section in the table"""
    start_idx = None
    end_idx = None
    
    # Search for the OTHER section header
    for i in range(header_row_idx, len(table)):
        row = table[i]
        if not row:
            continue
            
        # Join the row elements to check for section header
        row_text = ' '.join(str(cell) for cell in row if cell)
        if 'O T H E R' in row_text:
            start_idx = i + 2  # Skip the empty row after header
            break
    
    if start_idx is None:
        print("Could not find OTHER section start")
        return None, None
        
    # Search for the end of section (next section header or empty rows)
    section_markers = ['F R U I T S', 'R I C E', 'F I S H']
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

def find_fruits_section_boundaries(table, header_row_idx):
    """Find the start and end indices for the Fruits section in the table"""
    start_idx = None
    end_idx = None
    
    # Search for the FRUITS section header
    for i in range(header_row_idx, len(table)):
        row = table[i]
        if not row:
            continue
            
        # Join the row elements to check for section header
        row_text = ' '.join(str(cell) for cell in row if cell)
        if 'F R U I T S' in row_text:
            start_idx = i + 2  # Skip the empty row after header
            break
    
    if start_idx is None:
        print("Could not find FRUITS section start")
        return None, None
        
    # Search for the end of section (next section header or empty rows)
    section_markers = ['R I C E', 'F I S H']
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

def find_rice_section_boundaries(table, header_row_idx):
    """Find the start and end indices for the Rice section in the table"""
    start_idx = None
    end_idx = None
    
    # Search for the RICE section header
    for i in range(header_row_idx, len(table)):
        row = table[i]
        if not row:
            continue
            
        # Join the row elements to check for section header
        row_text = ' '.join(str(cell) for cell in row if cell)
        if 'R I C E' in row_text:
            start_idx = i + 4  # Skip the empty row after header
            break
    
    if start_idx is None:
        print("Could not find RICE section start")
        return None, None
        
    # Search for the end of section (next section header or empty rows)
    section_markers = ['F I S H']
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

def clean_price(row, index):
    """
    Clean and convert price string to float.
    Returns "N/A" for null or invalid values.
    """
    price_str = row[index]
    try:
        if not price_str or pd.isna(price_str) or price_str == 'n.a.':
            return "N/A"
            
        # Remove spaces
        if '.00' in price_str:
            price_str = price_str.split('.00')[0].replace(' ', '')
        else:
            price_str = 'N/A'
        
        # If the price starts with a comma, add a leading digit
        if price_str.startswith(',') and index > 1:
            if '.00' in row[index - 1]:
                price_str = row[index - 1].split('.00')[1] + price_str
            else:
                price_str = row[index - 1] + price_str
        # Now convert to float, after removing the comma
        float_value = float(price_str.replace(',', ''))
        returnValue = str(float_value) if float_value else "N/A"
        return returnValue
    except Exception as e:
        print(f"Error cleaning price {price_str}: {str(e)}")
        return "N/A"

def extract_prices(row):
    """
    Extract and clean price values from a row.
    Returns tuple of (pettah_wholesale_yesterday, pettah_wholesale_today,
                     dambulla_wholesale_yesterday, dambulla_wholesale_today,
                     pettah_retail_yesterday, pettah_retail_today,
                     dambulla_retail_yesterday, dambulla_retail_today,
                     narahenpita_retail_yesterday, narahenpita_retail_today)
    All values will be strings, with "N/A" for null values.
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
        pettah_wholesale_y = clean_price(row, 3) if len(row) > 3 else "N/A"
        pettah_wholesale_t = clean_price(row, 5) if len(row) > 5 else "N/A"
        
        # Extract Dambulla wholesale prices (columns 6 and 8)
        dambulla_wholesale_y = clean_price(row, 6) if len(row) > 6 else "N/A"
        dambulla_wholesale_t = clean_price(row, 8) if len(row) > 8 else "N/A"
        
        # Extract Pettah retail prices (columns 9 and 10)
        pettah_retail_y = clean_price(row, 9) if len(row) > 9 else "N/A"
        pettah_retail_t = clean_price(row, 10) if len(row) > 10 else "N/A"
        
        # Extract Dambulla retail prices (columns 12 and 14)
        dambulla_retail_y = clean_price(row, 12) if len(row) > 12 else "N/A"
        dambulla_retail_t = clean_price(row, 14) if len(row) > 14 else "N/A"
        
        # Extract Narahenpita retail prices (columns 16 and 18)
        narahenpita_retail_y = clean_price(row, 16) if len(row) > 16 else "N/A"
        narahenpita_retail_t = clean_price(row, 18) if len(row) > 18 else "N/A"
        
        # Print extracted Narahenpita prices for debugging
        print(f"Extracted Narahenpita prices - Yesterday: {narahenpita_retail_y}, Today: {narahenpita_retail_t}")
        
        return (pettah_wholesale_y, pettah_wholesale_t,
                dambulla_wholesale_y, dambulla_wholesale_t,
                pettah_retail_y, pettah_retail_t,
                dambulla_retail_y, dambulla_retail_t,
                narahenpita_retail_y, narahenpita_retail_t)
                
    except Exception as e:
        print(f"Error extracting prices: {str(e)}")
        return "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A"

def process_table_data(table):
    """Convert table data to a MongoDB-compatible format with proper column mapping"""
    if not table:
        return []

    # Find the header row index
    header_row_idx = None
    for idx, row in enumerate(table):
        if row and any('WHOLESALE' in str(cell).upper() for cell in row):
            header_row_idx = idx
            break

    if header_row_idx is None:
        print("Could not find header row")
        return []

    all_data = []

    # Process vegetables section
    veg_start_idx, veg_end_idx = find_section_boundaries(table, header_row_idx)
    if veg_start_idx is not None and veg_end_idx is not None:
        for row in table[veg_start_idx:veg_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    prices = extract_prices(row)
                    if any(prices):  # Only add if we have any price data
                        all_data.append({
                            'type': 'vegetables',
                            'item': item_name,
                            'pettah_wholesale': {
                                'yesterday': prices[0],
                                'today': prices[1]
                            },
                            'dambulla_wholesale': {
                                'yesterday': prices[2],
                                'today': prices[3]
                            },
                            'pettah_retail': {
                                'yesterday': prices[4],
                                'today': prices[5]
                            },
                            'dambulla_retail': {
                                'yesterday': prices[6],
                                'today': prices[7]
                            },
                            'narahenpita_retail': {
                                'yesterday': prices[8],
                                'today': prices[9]
                            },
                            'timestamp': datetime.now()
                        })

    # Process other section
    other_start_idx, other_end_idx = find_other_section_boundaries(table, header_row_idx)
    if other_start_idx is not None and other_end_idx is not None:
        for row in table[other_start_idx:other_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    prices = extract_prices(row)
                    if any(prices):  # Only add if we have any price data
                        all_data.append({
                            'type': 'other',
                            'item': item_name,
                            'pettah_wholesale': {
                                'yesterday': prices[0],
                                'today': prices[1]
                            },
                            'dambulla_wholesale': {
                                'yesterday': prices[2],
                                'today': prices[3]
                            },
                            'pettah_retail': {
                                'yesterday': prices[4],
                                'today': prices[5]
                            },
                            'dambulla_retail': {
                                'yesterday': prices[6],
                                'today': prices[7]
                            },
                            'narahenpita_retail': {
                                'yesterday': prices[8],
                                'today': prices[9]
                            },
                            'timestamp': datetime.now()
                        })

    # Process fruits section
    fruits_start_idx, fruits_end_idx = find_fruits_section_boundaries(table, header_row_idx)
    if fruits_start_idx is not None and fruits_end_idx is not None:
        for row in table[fruits_start_idx:fruits_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    prices = extract_prices(row)
                    if any(prices):  # Only add if we have any price data
                        all_data.append({
                            'type': 'fruits',
                            'item': item_name,
                            'pettah_wholesale': {
                                'yesterday': prices[0],
                                'today': prices[1]
                            },
                            'dambulla_wholesale': {
                                'yesterday': prices[2],
                                'today': prices[3]
                            },
                            'pettah_retail': {
                                'yesterday': prices[4],
                                'today': prices[5]
                            },
                            'dambulla_retail': {
                                'yesterday': prices[6],
                                'today': prices[7]
                            },
                            'narahenpita_retail': {
                                'yesterday': prices[8],
                                'today': prices[9]
                            },
                            'timestamp': datetime.now()
                        })

    # Process rice section
    rice_start_idx, rice_end_idx = find_rice_section_boundaries(table, header_row_idx)
    if rice_start_idx is not None and rice_end_idx is not None:
        for row in table[rice_start_idx:rice_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    prices = extract_prices(row)
                    if any(prices):  # Only add if we have any price data
                        all_data.append({
                            'type': 'rice',
                            'item': item_name,
                            'pettah_wholesale': {
                                'yesterday': prices[0],
                                'today': prices[1]
                            },
                            'marandagahamula_wholesale': {
                                'yesterday': prices[2],
                                'today': prices[3]
                            },
                            'pettah_retail': {
                                'yesterday': prices[4],
                                'today': prices[5]
                            },
                            'dambulla_retail': {
                                'yesterday': prices[6],
                                'today': prices[7]
                            },
                            'narahenpita_retail': {
                                'yesterday': prices[8],
                                'today': prices[9]
                            },
                            'timestamp': datetime.now()
                        })


    return all_data

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
            
            # Split the data into different sections
            vegetables_data = [item for item in processed_data if item['type'] == 'vegetables']
            other_data = [item for item in processed_data if item['type'] == 'other']
            fruits_data = [item for item in processed_data if item['type'] == 'fruits']
            rice_data = [item for item in processed_data if item['type'] == 'rice']
            
            # Create separate documents for each section
            documents = []
            
            if vegetables_data:
                vegetables_document = {
                    'date': date_obj,
                    'type': 'vegetables',
                    'page': 2,
                    'table_index': 0,
                    'data': vegetables_data
                }
                documents.append(vegetables_document)
                
            if other_data:
                other_document = {
                    'date': date_obj,
                    'type': 'other',
                    'page': 2,
                    'table_index': 1,
                    'data': other_data
                }
                documents.append(other_document)
                
            if fruits_data:
                fruits_document = {
                    'date': date_obj,
                    'type': 'fruits',
                    'page': 2,
                    'table_index': 2,
                    'data': fruits_data
                }
                documents.append(fruits_document)
                
            if rice_data:
                rice_document = {
                    'date': date_obj,
                    'type': 'rice',
                    'page': 2,
                    'table_index': 3,
                    'data': rice_data
                }
                documents.append(rice_document)
            
            return documents
            
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
