import os
import re
import pdfplumber
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta

# MongoDB connection
client = MongoClient('mongodb://root:secret@localhost:27017/')
db = client['central_bank']
collection = db['row_data']

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

def find_fish_section_boundaries(table, header_row_idx):
    """Find the start and end indices for the Fish section in the table"""
    start_idx = None
    end_idx = None
    
    # Search for the FISH section header
    for i in range(header_row_idx, len(table)):
        row = table[i]
        if not row:
            continue
            
        # Join the row elements to check for section header
        row_text = ' '.join(str(cell) for cell in row if cell)
        if 'F I S H' in row_text:
            start_idx = i + 4  # Skip the empty row after header
            break
    
    if start_idx is None:
        print("Could not find FISH section start")
        return None, None
        
    # Search for the end of section (next section header or empty rows)
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
                
        if end_idx:
            break
            
    # If no clear end found, use the end of the table
    if not end_idx:
        end_idx = len(table)
    
    # Skip last two rows
    if end_idx and end_idx - start_idx > 2:
        end_idx = end_idx - 3
    
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

def extract_prices(row, is_monday_format=False):
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
        print(f"Is Monday format: {is_monday_format}")
        
        # For Monday reports, the indices are different due to "Last Friday" format
        if is_monday_format:

            if len(row) == 13:
                # Wholesale prices
                pettah_wholesale_y = clean_price(row, 2) if len(row) > 2 else "N/A"  # Last Friday
                pettah_wholesale_t = clean_price(row, 3) if len(row) > 3 else "N/A"  # Today
                dambulla_wholesale_y = clean_price(row, 4) if len(row) > 4 else "N/A"  # Last Friday
                dambulla_wholesale_t = clean_price(row, 6) if len(row) > 6 else "N/A"  # Today
                
                # Retail prices - Fixed indices based on actual data structure
                pettah_retail_y = clean_price(row, 7) if len(row) > 7 else "N/A"  # Last Friday
                pettah_retail_t = clean_price(row, 8) if len(row) > 8 else "N/A"  # Today
                dambulla_retail_y = clean_price(row, 9) if len(row) > 9 else "N/A"  # Last Friday
                dambulla_retail_t = clean_price(row, 10) if len(row) > 10 else "N/A"  # Today
                narahenpita_retail_y = clean_price(row, 11) if len(row) > 11 else "N/A"  # Last Friday
                narahenpita_retail_t = clean_price(row, 12) if len(row) > 12 else "N/A"  # Today

                print(f"Monday format prices for {row[0]}:")

                print(f"Pettah wholesale: Yesterday (2)={row[2] if len(row)>2 else 'N/A'}, Today (4)={row[3] if len(row)>3 else 'N/A'}")
                print(f"Dambulla wholesale: Yesterday (5)={row[4] if len(row)>4 else 'N/A'}, Today (7)={row[6] if len(row)>6 else 'N/A'}")
                print(f"Pettah wholesale: Yesterday={pettah_wholesale_y}, Today={pettah_wholesale_t}")
                print(f"Dambulla wholesale: Yesterday={dambulla_wholesale_y}, Today={dambulla_wholesale_t}")

                print(f"Pettah retail: Last Friday (9)={row[7] if len(row)>7 else 'N/A'}, Today (10)={row[8] if len(row)>8 else 'N/A'}")
                print(f"Dambulla retail: Last Friday (11)={row[9] if len(row)>9 else 'N/A'}, Today (12)={row[10] if len(row)>10 else 'N/A'}")
                print(f"Narahenpita retail: Last Friday (13)={row[11] if len(row)>11 else 'N/A'}, Today (18)={row[12] if len(row)>12 else 'N/A'}")
                print(f"Pettah retail: Last Friday={pettah_retail_y}, Today={pettah_retail_t}")
                print(f"Dambulla retail: Last Friday={dambulla_retail_y}, Today={dambulla_retail_t}")
                print(f"Narahenpita retail: Last Friday={narahenpita_retail_y}, Today={narahenpita_retail_t}")
            if len(row) == 14:
                # Wholesale prices
                pettah_wholesale_y = clean_price(row, 2) if len(row) > 2 else "N/A"  # Last Friday
                pettah_wholesale_t = clean_price(row, 3) if len(row) > 3 else "N/A"  # Today
                dambulla_wholesale_y = clean_price(row, 4) if len(row) > 4 else "N/A"  # Last Friday
                dambulla_wholesale_t = clean_price(row, 6) if len(row) > 6 else "N/A"  # Today
                
                # Retail prices - Fixed indices based on actual data structure
                pettah_retail_y = clean_price(row, 8) if len(row) > 8 else "N/A"  # Last Friday
                pettah_retail_t = clean_price(row, 9) if len(row) > 9 else "N/A"  # Today
                dambulla_retail_y = clean_price(row, 10) if len(row) > 10 else "N/A"  # Last Friday
                dambulla_retail_t = clean_price(row, 11) if len(row) > 11 else "N/A"  # Today
                narahenpita_retail_y = clean_price(row, 12) if len(row) > 12 else "N/A"  # Last Friday
                narahenpita_retail_t = clean_price(row, 13) if len(row) > 13 else "N/A"  # Today

                print(f"Monday format prices for {row[0]}:")

                print(f"Pettah wholesale: Yesterday (2)={row[2] if len(row)>2 else 'N/A'}, Today (4)={row[3] if len(row)>3 else 'N/A'}")
                print(f"Dambulla wholesale: Yesterday (5)={row[4] if len(row)>4 else 'N/A'}, Today (7)={row[6] if len(row)>6 else 'N/A'}")
                print(f"Pettah wholesale: Yesterday={pettah_wholesale_y}, Today={pettah_wholesale_t}")
                print(f"Dambulla wholesale: Yesterday={dambulla_wholesale_y}, Today={dambulla_wholesale_t}")

                print(f"Pettah retail: Last Friday (8)={row[8] if len(row)>8 else 'N/A'}, Today (9)={row[9] if len(row)>9 else 'N/A'}")
                print(f"Dambulla retail: Last Friday (10)={row[10] if len(row)>10 else 'N/A'}, Today (11)={row[11] if len(row)>11 else 'N/A'}")
                print(f"Narahenpita retail: Last Friday (12)={row[12] if len(row)>12 else 'N/A'}, Today (13)={row[13] if len(row)>13 else 'N/A'}")
                print(f"Pettah retail: Last Friday={pettah_retail_y}, Today={pettah_retail_t}")
                print(f"Dambulla retail: Last Friday={dambulla_retail_y}, Today={dambulla_retail_t}")
                print(f"Narahenpita retail: Last Friday={narahenpita_retail_y}, Today={narahenpita_retail_t}")
            elif len(row) == 18:
                # Wholesale prices
                pettah_wholesale_y = clean_price(row, 3) if len(row) > 3 else "N/A"  # Last Friday
                pettah_wholesale_t = clean_price(row, 5) if len(row) > 5 else "N/A"  # Today
                dambulla_wholesale_y = clean_price(row, 6) if len(row) > 6 else "N/A"  # Last Friday
                dambulla_wholesale_t = clean_price(row, 8) if len(row) > 8 else "N/A"  # Today
                
                # Retail prices - Fixed indices based on actual data structure
                pettah_retail_y = clean_price(row, 9) if len(row) > 9 else "N/A"  # Last Friday
                pettah_retail_t = clean_price(row, 10) if len(row) > 10 else "N/A"  # Today
                dambulla_retail_y = clean_price(row, 12) if len(row) > 12 else "N/A"  # Last Friday
                dambulla_retail_t = clean_price(row, 13) if len(row) > 13 else "N/A"  # Today
                narahenpita_retail_y = clean_price(row, 15) if len(row) > 15 else "N/A"  # Last Friday
                narahenpita_retail_t = clean_price(row, 17) if len(row) > 17 else "N/A"  # Today

                print(f"Monday format prices for {row[0]}:")

                print(f"Pettah wholesale: Yesterday (3)={row[3] if len(row)>3 else 'N/A'}, Today (5)={row[5] if len(row)>5 else 'N/A'}")
                print(f"Dambulla wholesale: Yesterday (6)={row[6] if len(row)>6 else 'N/A'}, Today (8)={row[8] if len(row)>8 else 'N/A'}")
                print(f"Pettah wholesale: Yesterday={pettah_wholesale_y}, Today={pettah_wholesale_t}")
                print(f"Dambulla wholesale: Yesterday={dambulla_wholesale_y}, Today={dambulla_wholesale_t}")

                print(f"Pettah retail: Last Friday (9)={row[9] if len(row)>9 else 'N/A'}, Today (10)={row[10] if len(row)>10 else 'N/A'}")
                print(f"Dambulla retail: Last Friday (12)={row[12] if len(row)>12 else 'N/A'}, Today (13)={row[13] if len(row)>13 else 'N/A'}")
                print(f"Narahenpita retail: Last Friday (15)={row[15] if len(row)>15 else 'N/A'}, Today (17)={row[17] if len(row)>17 else 'N/A'}")
                print(f"Pettah retail: Last Friday={pettah_retail_y}, Today={pettah_retail_t}")
                print(f"Dambulla retail: Last Friday={dambulla_retail_y}, Today={dambulla_retail_t}")
                print(f"Narahenpita retail: Last Friday={narahenpita_retail_y}, Today={narahenpita_retail_t}")
            else:
                # Wholesale prices
                pettah_wholesale_y = clean_price(row, 3) if len(row) > 3 else "N/A"  # Last Friday
                pettah_wholesale_t = clean_price(row, 5) if len(row) > 5 else "N/A"  # Today
                dambulla_wholesale_y = clean_price(row, 6) if len(row) > 6 else "N/A"  # Last Friday
                dambulla_wholesale_t = clean_price(row, 8) if len(row) > 8 else "N/A"  # Today
            
                # Retail prices - Fixed indices based on actual data structure
                pettah_retail_y = clean_price(row, 10) if len(row) > 10 else "N/A"  # Last Friday
                pettah_retail_t = clean_price(row, 11) if len(row) > 11 else "N/A"  # Today
                dambulla_retail_y = clean_price(row, 13) if len(row) > 13 else "N/A"  # Last Friday
                dambulla_retail_t = clean_price(row, 14) if len(row) > 14 else "N/A"  # Today
                narahenpita_retail_y = clean_price(row, 16) if len(row) > 16 else "N/A"  # Last Friday
                narahenpita_retail_t = clean_price(row, 18) if len(row) > 18 else "N/A"  # Today
            
                print(f"Monday format prices for {row[0]}:")

                print(f"Pettah wholesale: Yesterday (4)={row[3] if len(row)>3 else 'N/A'}, Today (5)={row[5] if len(row)>5 else 'N/A'}")
                print(f"Dambulla wholesale: Yesterday (6)={row[6] if len(row)>6 else 'N/A'}, Today (8)={row[8] if len(row)>8 else 'N/A'}")
                print(f"Pettah wholesale: Yesterday={pettah_wholesale_y}, Today={pettah_wholesale_t}")
                print(f"Dambulla wholesale: Yesterday={dambulla_wholesale_y}, Today={dambulla_wholesale_t}")

                print(f"Pettah retail: Last Friday (10)={row[10] if len(row)>10 else 'N/A'}, Today (11)={row[11] if len(row)>11 else 'N/A'}")
                print(f"Dambulla retail: Last Friday (13)={row[13] if len(row)>13 else 'N/A'}, Today (14)={row[14] if len(row)>14 else 'N/A'}")
                print(f"Narahenpita retail: Last Friday (16)={row[16] if len(row)>16 else 'N/A'}, Today (18)={row[18] if len(row)>18 else 'N/A'}")
                print(f"Pettah retail: Last Friday={pettah_retail_y}, Today={pettah_retail_t}")
                print(f"Dambulla retail: Last Friday={dambulla_retail_y}, Today={dambulla_retail_t}")
                print(f"Narahenpita retail: Last Friday={narahenpita_retail_y}, Today={narahenpita_retail_t}")
        else:
            if len(row) == 13:
                # Wholesale prices
                pettah_wholesale_y = clean_price(row, 2) if len(row) > 2 else "N/A"  # Yesterday
                pettah_wholesale_t = clean_price(row, 3) if len(row) > 3 else "N/A"  # Today
                dambulla_wholesale_y = clean_price(row, 4) if len(row) > 4 else "N/A"  # Yesterday
                dambulla_wholesale_t = clean_price(row, 6) if len(row) > 6 else "N/A"  # Today
                
                # Retail prices - Fixed indices based on actual data structure
                pettah_retail_y = clean_price(row, 7) if len(row) > 7 else "N/A"  # Yesterday
                pettah_retail_t = clean_price(row, 8) if len(row) > 8 else "N/A"  # Today
                dambulla_retail_y = clean_price(row, 9) if len(row) > 9 else "N/A"  # Yesterday
                dambulla_retail_t = clean_price(row, 10) if len(row) > 10 else "N/A"  # Today
                narahenpita_retail_y = clean_price(row, 11) if len(row) > 11 else "N/A"  # Yesterday
                narahenpita_retail_t = clean_price(row, 12) if len(row) > 12 else "N/A"  # Today

                print(f"Monday format prices for {row[0]}:")

                print(f"Pettah wholesale: Yesterday (2)={row[2] if len(row)>2 else 'N/A'}, Today (4)={row[3] if len(row)>3 else 'N/A'}")
                print(f"Dambulla wholesale: Yesterday (5)={row[4] if len(row)>4 else 'N/A'}, Today (7)={row[6] if len(row)>6 else 'N/A'}")
                print(f"Pettah wholesale: Yesterday={pettah_wholesale_y}, Today={pettah_wholesale_t}")
                print(f"Dambulla wholesale: Yesterday={dambulla_wholesale_y}, Today={dambulla_wholesale_t}")

                print(f"Pettah retail: Last Friday (9)={row[7] if len(row)>7 else 'N/A'}, Today (10)={row[8] if len(row)>8 else 'N/A'}")
                print(f"Dambulla retail: Last Friday (11)={row[9] if len(row)>9 else 'N/A'}, Today (12)={row[10] if len(row)>10 else 'N/A'}")
                print(f"Narahenpita retail: Last Friday (13)={row[11] if len(row)>11 else 'N/A'}, Today (18)={row[12] if len(row)>12 else 'N/A'}")
                print(f"Pettah retail: Last Friday={pettah_retail_y}, Today={pettah_retail_t}")
                print(f"Dambulla retail: Last Friday={dambulla_retail_y}, Today={dambulla_retail_t}")
                print(f"Narahenpita retail: Last Friday={narahenpita_retail_y}, Today={narahenpita_retail_t}")
            elif len(row) == 14:
                # Wholesale prices
                pettah_wholesale_y = clean_price(row, 2) if len(row) > 2 else "N/A"  # Yesterday
                pettah_wholesale_t = clean_price(row, 3) if len(row) > 3 else "N/A"  # Today
                dambulla_wholesale_y = clean_price(row, 4) if len(row) > 4 else "N/A"  # Yesterday
                dambulla_wholesale_t = clean_price(row, 6) if len(row) > 6 else "N/A"  # Today
                
                # Retail prices - Fixed indices based on actual data structure
                pettah_retail_y = clean_price(row, 8) if len(row) > 8 else "N/A"  # Yesterday
                pettah_retail_t = clean_price(row, 9) if len(row) > 9 else "N/A"  # Today
                dambulla_retail_y = clean_price(row, 10) if len(row) > 10 else "N/A"  # Yesterday
                dambulla_retail_t = clean_price(row, 11) if len(row) > 11 else "N/A"  # Today
                narahenpita_retail_y = clean_price(row, 12) if len(row) > 12 else "N/A"  # Yesterday
                narahenpita_retail_t = clean_price(row, 13) if len(row) > 13 else "N/A"  # Today

                print(f"Monday format prices for {row[0]}:")

                print(f"Pettah wholesale: Yesterday (2)={row[2] if len(row)>2 else 'N/A'}, Today (3)={row[3] if len(row)>3 else 'N/A'}")
                print(f"Dambulla wholesale: Yesterday (4)={row[4] if len(row)>4 else 'N/A'}, Today (6)={row[6] if len(row)>6 else 'N/A'}")
                print(f"Pettah wholesale: Yesterday={pettah_wholesale_y}, Today={pettah_wholesale_t}")
                print(f"Dambulla wholesale: Yesterday={dambulla_wholesale_y}, Today={dambulla_wholesale_t}")

                print(f"Pettah retail: Last Friday (8)={row[8] if len(row)>8 else 'N/A'}, Today (9)={row[9] if len(row)>9 else 'N/A'}")
                print(f"Dambulla retail: Last Friday (10)={row[10] if len(row)>10 else 'N/A'}, Today (11)={row[11] if len(row)>11 else 'N/A'}")
                print(f"Narahenpita retail: Last Friday (12)={row[12] if len(row)>12 else 'N/A'}, Today (13)={row[13] if len(row)>13 else 'N/A'}")
                print(f"Pettah retail: Last Friday={pettah_retail_y}, Today={pettah_retail_t}")
                print(f"Dambulla retail: Last Friday={dambulla_retail_y}, Today={dambulla_retail_t}")
                print(f"Narahenpita retail: Last Friday={narahenpita_retail_y}, Today={narahenpita_retail_t}")
            elif len(row) == 15:
                # Wholesale prices
                pettah_wholesale_y = clean_price(row, 2) if len(row) > 2 else "N/A"  # Yesterday
                pettah_wholesale_t = clean_price(row, 3) if len(row) > 3 else "N/A"  # Today
                dambulla_wholesale_y = clean_price(row, 4) if len(row) > 4 else "N/A"  # Yesterday
                dambulla_wholesale_t = clean_price(row, 6) if len(row) > 6 else "N/A"  # Today
                
                # Retail prices - Fixed indices based on actual data structure
                pettah_retail_y = clean_price(row, 7) if len(row) > 7 else "N/A"  # Yesterday
                pettah_retail_t = clean_price(row, 8) if len(row) > 8 else "N/A"  # Today
                dambulla_retail_y = clean_price(row, 9) if len(row) > 9 else "N/A"  # Yesterday
                dambulla_retail_t = clean_price(row, 10) if len(row) > 10 else "N/A"  # Today
                narahenpita_retail_y = clean_price(row, 12) if len(row) > 12 else "N/A"  # Yesterday
                narahenpita_retail_t = clean_price(row, 14) if len(row) > 14 else "N/A"  # Today

                print(f"Monday format prices for {row[0]}:")

                print(f"Pettah wholesale: Yesterday (2)={row[2] if len(row)>2 else 'N/A'}, Today (3)={row[3] if len(row)>3 else 'N/A'}")
                print(f"Dambulla wholesale: Yesterday (4)={row[4] if len(row)>4 else 'N/A'}, Today (6)={row[6] if len(row)>6 else 'N/A'}")
                print(f"Pettah wholesale: Yesterday={pettah_wholesale_y}, Today={pettah_wholesale_t}")
                print(f"Dambulla wholesale: Yesterday={dambulla_wholesale_y}, Today={dambulla_wholesale_t}")

                print(f"Pettah retail: Last Friday (7)={row[7] if len(row)>7 else 'N/A'}, Today (8)={row[8] if len(row)>8 else 'N/A'}")
                print(f"Dambulla retail: Last Friday (9)={row[9] if len(row)>9 else 'N/A'}, Today (10)={row[10] if len(row)>10 else 'N/A'}")
                print(f"Narahenpita retail: Last Friday (12)={row[12] if len(row)>12 else 'N/A'}, Today (14)={row[14] if len(row)>14 else 'N/A'}")
                print(f"Pettah retail: Last Friday={pettah_retail_y}, Today={pettah_retail_t}")
                print(f"Dambulla retail: Last Friday={dambulla_retail_y}, Today={dambulla_retail_t}")
                print(f"Narahenpita retail: Last Friday={narahenpita_retail_y}, Today={narahenpita_retail_t}")
            elif len(row) == 16:
                # Wholesale prices
                pettah_wholesale_y = clean_price(row, 3) if len(row) > 3 else "N/A"  # Yesterday
                pettah_wholesale_t = clean_price(row, 4) if len(row) > 4 else "N/A"  # Today
                dambulla_wholesale_y = clean_price(row, 5) if len(row) > 5 else "N/A"  # Yesterday
                dambulla_wholesale_t = clean_price(row, 7) if len(row) > 7 else "N/A"  # Today
                
                # Retail prices - Fixed indices based on actual data structure
                pettah_retail_y = clean_price(row, 8) if len(row) > 8 else "N/A"  # Yesterday
                pettah_retail_t = clean_price(row, 9) if len(row) > 9 else "N/A"  # Today
                dambulla_retail_y = clean_price(row, 10) if len(row) > 10 else "N/A"  # Yesterday
                dambulla_retail_t = clean_price(row, 11) if len(row) > 11 else "N/A"  # Today
                narahenpita_retail_y = clean_price(row, 13) if len(row) > 13 else "N/A"  # Yesterday
                narahenpita_retail_t = clean_price(row, 15) if len(row) > 15 else "N/A"  # Today

                print(f"Monday format prices for {row[0]}:")

                print(f"Pettah wholesale: Yesterday (3)={row[3] if len(row)>3 else 'N/A'}, Today (4)={row[4] if len(row)>4 else 'N/A'}")
                print(f"Dambulla wholesale: Yesterday (5)={row[5] if len(row)>5 else 'N/A'}, Today (7)={row[7] if len(row)>7 else 'N/A'}")
                print(f"Pettah wholesale: Yesterday={pettah_wholesale_y}, Today={pettah_wholesale_t}")
                print(f"Dambulla wholesale: Yesterday={dambulla_wholesale_y}, Today={dambulla_wholesale_t}")

                print(f"Pettah retail: Last Friday (8)={row[8] if len(row)>8 else 'N/A'}, Today (9)={row[9] if len(row)>9 else 'N/A'}")
                print(f"Dambulla retail: Last Friday (10)={row[10] if len(row)>10 else 'N/A'}, Today (11)={row[11] if len(row)>11 else 'N/A'}")
                print(f"Narahenpita retail: Last Friday (13)={row[13] if len(row)>13 else 'N/A'}, Today (15)={row[15] if len(row)>15 else 'N/A'}")
                print(f"Pettah retail: Last Friday={pettah_retail_y}, Today={pettah_retail_t}")
                print(f"Dambulla retail: Last Friday={dambulla_retail_y}, Today={dambulla_retail_t}")
                print(f"Narahenpita retail: Last Friday={narahenpita_retail_y}, Today={narahenpita_retail_t}")
            elif len(row) == 17:
                # Wholesale prices
                pettah_wholesale_y = clean_price(row, 3) if len(row) > 3 else "N/A"
                pettah_wholesale_t = clean_price(row, 5) if len(row) > 5 else "N/A"
                dambulla_wholesale_y = clean_price(row, 6) if len(row) > 6 else "N/A"
                dambulla_wholesale_t = clean_price(row, 8) if len(row) > 8 else "N/A"

                # Retail prices
                pettah_retail_y = clean_price(row, 9) if len(row) > 9 else "N/A"
                pettah_retail_t = clean_price(row, 10) if len(row) > 10 else "N/A"
                dambulla_retail_y = clean_price(row, 11) if len(row) > 11 else "N/A"
                dambulla_retail_t = clean_price(row, 12) if len(row) > 12 else "N/A"
                narahenpita_retail_y = clean_price(row, 14) if len(row) > 14 else "N/A"
                narahenpita_retail_t = clean_price(row, 16) if len(row) > 16 else "N/A"
                
                print(f"Regular format prices for {row[0]} length={len(row)}")
                print(f"Raw values at retail indices:")
                print(f"Pettah retail: Yesterday (9)={row[9] if len(row)>9 else 'N/A'}, Today (10)={row[10] if len(row)>10 else 'N/A'}")
                print(f"Dambulla retail: Yesterday (11)={row[11] if len(row)>11 else 'N/A'}, Today (12)={row[12] if len(row)>12 else 'N/A'}")
                print(f"Narahenpita retail: Yesterday (14)={row[14] if len(row)>14 else 'N/A'}, Today (16)={row[16] if len(row)>16 else 'N/A'}")
                print(f"Cleaned values:")
                print(f"Pettah retail: Yesterday={pettah_retail_y}, Today={pettah_retail_t}")
                print(f"Dambulla retail: Yesterday={dambulla_retail_y}, Today={dambulla_retail_t}")
                print(f"Narahenpita retail: Yesterday={narahenpita_retail_y}, Today={narahenpita_retail_t}")
            else:
                # Regular report indices (Yesterday/Today format)
                pettah_wholesale_y = clean_price(row, 3) if len(row) > 3 else "N/A"
                pettah_wholesale_t = clean_price(row, 5) if len(row) > 5 else "N/A"
                dambulla_wholesale_y = clean_price(row, 6) if len(row) > 6 else "N/A"
                dambulla_wholesale_t = clean_price(row, 8) if len(row) > 8 else "N/A"
                pettah_retail_y = clean_price(row, 9) if len(row) > 9 else "N/A"
                pettah_retail_t = clean_price(row, 10) if len(row) > 10 else "N/A"
                dambulla_retail_y = clean_price(row, 12) if len(row) > 12 else "N/A"
                dambulla_retail_t = clean_price(row, 14) if len(row) > 14 else "N/A"
                narahenpita_retail_y = clean_price(row, 16) if len(row) > 16 else "N/A"
                narahenpita_retail_t = clean_price(row, 18) if len(row) > 18 else "N/A"
                
                print(f"Regular format prices for {row[0]} length={len(row)}")
                print(f"Raw values at retail indices:")
                print(f"Pettah retail: Yesterday (9)={row[9] if len(row)>9 else 'N/A'}, Today (10)={row[10] if len(row)>10 else 'N/A'}")
                print(f"Dambulla retail: Yesterday (12)={row[12] if len(row)>12 else 'N/A'}, Today (14)={row[14] if len(row)>14 else 'N/A'}")
                print(f"Narahenpita retail: Yesterday (16)={row[16] if len(row)>16 else 'N/A'}, Today (18)={row[18] if len(row)>18 else 'N/A'}")
                print(f"Cleaned values:")
                print(f"Pettah retail: Yesterday={pettah_retail_y}, Today={pettah_retail_t}")
                print(f"Dambulla retail: Yesterday={dambulla_retail_y}, Today={dambulla_retail_t}")
                print(f"Narahenpita retail: Yesterday={narahenpita_retail_y}, Today={narahenpita_retail_t}")
        
        return (pettah_wholesale_y, pettah_wholesale_t,
                dambulla_wholesale_y, dambulla_wholesale_t,
                pettah_retail_y, pettah_retail_t,
                dambulla_retail_y, dambulla_retail_t,
                narahenpita_retail_y, narahenpita_retail_t)
                
    except Exception as e:
        print(f"Error extracting prices: {str(e)}")
        return ("N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A")

def process_table_data(table):
    """Convert table data to a MongoDB-compatible format with proper column mapping"""
    if not table:
        return []

    # Find the header row index
    header_row_idx = None
    is_monday_format = False
    
    # First find the header row with "WHOLESALE"
    for idx, row in enumerate(table):
        if row and any('WHOLESALE' in str(cell).upper() for cell in row):
            header_row_idx = idx
            break

    if header_row_idx is None:
        print("Could not find header row")
        return []

    # Now look for "Last Friday" in the next few rows after header
    for i in range(header_row_idx, min(header_row_idx + 10, len(table))):
        if i < len(table):
            row = table[i]
            # Join all cells in the row to handle split text
            row_text = ' '.join(str(cell) for cell in row if cell)
            if 'LAST' in row_text.upper() and 'FRIDAY' in row_text.upper():
                is_monday_format = True
                break
            # Also check individual cells for "Last Friday"
            for cell in row:
                if cell and 'LAST' in str(cell).upper() and 'FRIDAY' in str(cell).upper():
                    is_monday_format = True
                    break

    print(f"Header format: {'Monday (Last Friday)' if is_monday_format else 'Regular (Yesterday)'}")
    print(f"Header row index: {header_row_idx}")
    if header_row_idx is not None and header_row_idx + 3 < len(table):
        print(f"Header rows:")
        for i in range(header_row_idx, header_row_idx + 4):
            if i < len(table):
                print(f"Row {i}: {table[i]}")

    all_data = []

    # Process vegetables section
    veg_start_idx, veg_end_idx = find_section_boundaries(table, header_row_idx)
    if veg_start_idx is not None and veg_end_idx is not None:
        for row in table[veg_start_idx:veg_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    print(f"\nProcessing row for item: {item_name}")
                    print(f"Is Monday format: {is_monday_format}")
                    prices = extract_prices(row, is_monday_format)
                    if any(prices):  # Only add if we have any price data
                        print(f"Extracted prices for {item_name}:")
                        print(f"Pettah retail - Yesterday: {prices[4]}, Today: {prices[5]}")
                        print(f"Dambulla retail - Yesterday: {prices[6]}, Today: {prices[7]}")
                        print(f"Narahenpita retail - Yesterday: {prices[8]}, Today: {prices[9]}")
                        
                        data_item = {
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
                        }
                        all_data.append(data_item)

    # Process other section with the same format changes
    other_start_idx, other_end_idx = find_other_section_boundaries(table, header_row_idx)
    if other_start_idx is not None and other_end_idx is not None:
        for row in table[other_start_idx:other_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    print(f"\nProcessing row for item: {item_name}")
                    print(f"Is Monday format: {is_monday_format}")
                    prices = extract_prices(row, is_monday_format)
                    if any(prices):  # Only add if we have any price data
                        print(f"Extracted prices for {item_name}:")
                        print(f"Pettah retail - Yesterday: {prices[4]}, Today: {prices[5]}")
                        print(f"Dambulla retail - Yesterday: {prices[6]}, Today: {prices[7]}")
                        print(f"Narahenpita retail - Yesterday: {prices[8]}, Today: {prices[9]}")
                        
                        data_item = {
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
                        }
                        all_data.append(data_item)

    # Process fruits section
    fruits_start_idx, fruits_end_idx = find_fruits_section_boundaries(table, header_row_idx)
    if fruits_start_idx is not None and fruits_end_idx is not None:
        for row in table[fruits_start_idx:fruits_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    print(f"\nProcessing row for item: {item_name}")
                    print(f"Is Monday format: {is_monday_format}")
                    prices = extract_prices(row, is_monday_format)
                    if any(prices):  # Only add if we have any price data
                        print(f"Extracted prices for {item_name}:")
                        print(f"Pettah retail - Yesterday: {prices[4]}, Today: {prices[5]}")
                        print(f"Dambulla retail - Yesterday: {prices[6]}, Today: {prices[7]}")
                        print(f"Narahenpita retail - Yesterday: {prices[8]}, Today: {prices[9]}")
                        
                        data_item = {
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
                        }
                        all_data.append(data_item)

    # Process rice section
    rice_start_idx, rice_end_idx = find_rice_section_boundaries(table, header_row_idx)
    if rice_start_idx is not None and rice_end_idx is not None:
        for row in table[rice_start_idx:rice_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    print(f"\nProcessing row for item: {item_name}")
                    print(f"Is Monday format: {is_monday_format}")
                    prices = extract_prices(row, is_monday_format)
                    if any(prices):  # Only add if we have any price data
                        print(f"Extracted prices for {item_name}:")
                        print(f"Pettah retail - Yesterday: {prices[4]}, Today: {prices[5]}")
                        print(f"Dambulla retail - Yesterday: {prices[6]}, Today: {prices[7]}")
                        print(f"Narahenpita retail - Yesterday: {prices[8]}, Today: {prices[9]}")
                        
                        data_item = {
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
                        }
                        all_data.append(data_item)

    # Process fish section
    fish_start_idx, fish_end_idx = find_fish_section_boundaries(table, header_row_idx)
    if fish_start_idx is not None and fish_end_idx is not None:
        for row in table[fish_start_idx:fish_end_idx]:
            if row and any(row):  # Skip empty rows
                item_name = str(row[0]).strip() if row[0] else ""
                if item_name and item_name.lower() != "item":
                    print(f"\nProcessing row for item: {item_name}")
                    print(f"Is Monday format: {is_monday_format}")
                    prices = extract_prices(row, is_monday_format)
                    if any(prices):  # Only add if we have any price data
                        print(f"Extracted prices for {item_name}:")
                        print(f"Pettah retail - Yesterday: {prices[4]}, Today: {prices[5]}")
                        print(f"Dambulla retail - Yesterday: {prices[6]}, Today: {prices[7]}")
                        print(f"Narahenpita retail - Yesterday: {prices[8]}, Today: {prices[9]}")
                        
                        data_item = {
                            'type': 'fish',
                            'item': item_name,
                            'peliyagoda_wholesale': {
                                'yesterday': prices[0],
                                'today': prices[1]
                            },
                            'negombo_wholesale': {
                                'yesterday': prices[2],
                                'today': prices[3]
                            },
                            'pettah_retail': {
                                'yesterday': prices[4],
                                'today': prices[5]
                            },
                            'negombo_retail': {
                                'yesterday': prices[6],
                                'today': prices[7]
                            },
                            'narahenpita_retail': {
                                'yesterday': prices[8],
                                'today': prices[9]
                            },
                            'timestamp': datetime.now()
                        }
                        all_data.append(data_item)

    return all_data

def get_last_friday(date):
    """Get the date of last Friday for a given date"""
    days_since_friday = (date.weekday() - 4) % 7
    last_friday = date - timedelta(days=days_since_friday)
    return last_friday

def is_monday(date):
    """Check if given date is a Monday"""
    return date.weekday() == 0

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
            
            # Get the date from filename (both formats: price_report_YYYYMMDD.pdf and price_report_YYYYMMDD_e.pdf)
            filename = os.path.basename(pdf_path)
            
            # Try both patterns
            match = re.match(r'price_report_(\d{8})\.pdf', filename) or re.match(r'price_report_(\d{8})_e\.pdf', filename)
            
            if not match:
                print(f"Error: Filename {filename} does not match expected format")
                return None
                
            date_str = match.group(1)  # Extract the date part (YYYYMMDD)
            try:
                date_obj = datetime.strptime(date_str, '%Y%m%d')
            except ValueError:
                print(f"Error: Invalid date format in filename {filename}")
                return None
            
            # Process the table data
            processed_data = process_table_data(table)
            
            # Split the data into different sections
            vegetables_data = [item for item in processed_data if item['type'] == 'vegetables']
            other_data = [item for item in processed_data if item['type'] == 'other']
            fruits_data = [item for item in processed_data if item['type'] == 'fruits']
            rice_data = [item for item in processed_data if item['type'] == 'rice']
            fish_data = [item for item in processed_data if item['type'] == 'fish']
            
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
                
            if fish_data:
                fish_document = {
                    'date': date_obj,
                    'type': 'fish',
                    'page': 2,
                    'table_index': 4,
                    'data': fish_data
                }
                documents.append(fish_document)
            
            return documents
            
    except Exception as e:
        print(f"Error extracting data from {pdf_path}: {str(e)}")
        return None

def main():
    # Create necessary directories if they don't exist
    os.makedirs('reports', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    
    # Define the regex patterns for both PDF file formats
    pattern = r'^price_report_\d{8}(?:_e)?\.pdf$'
    
    # Process all PDF files in the data directory
    pdf_dir = 'data'
    for filename in os.listdir(pdf_dir):
        # Check if the file matches our pattern and is a file (not directory)
        if re.match(pattern, filename) and os.path.isfile(os.path.join(pdf_dir, filename)):
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
                
                # Move the processed file to the processed folder
                processed_path = os.path.join(pdf_dir, 'processed', filename)
                os.rename(pdf_path, processed_path)
                print(f"Successfully processed and stored data from {filename}")
                print(f"Moved {filename} to processed folder")
            else:
                print(f"Failed to process {filename}")
        else:
            if filename.endswith('.pdf'):
                print(f"Skipping {filename} as it doesn't match the required format")

if __name__ == "__main__":
    main()
