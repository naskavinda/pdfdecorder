from pymongo import MongoClient
from datetime import datetime
import os

# MongoDB connection
client = MongoClient('mongodb://root:secret@localhost:27017/')
db = client['pdf_data']
collection = db['extracted_tables']

def is_vegetable_section(row):
    """Check if this row indicates the vegetable section"""
    for col in row.values():
        if isinstance(col, str) and 'V  E  G  E  T  A  B  L  E  S' in col:
            return True
    return False

def format_price(price_str):
    """Format price string to proper format"""
    try:
        price_float = float(price_str.replace(',', ''))
        return f"{price_float:,.2f}"
    except (ValueError, TypeError):
        return None

def extract_wholesale_prices(table_data):
    """
    Extract wholesale prices from the table for both Pettah and Dambulla (Today's prices)
    """
    prices = []
    rows = table_data['data']['rows']
    
    # Find the vegetable section
    veg_section_start = False
    other_section_start = False
    
    # Find the header row to identify price columns
    pettah_today_col = 'col_6'  # Today's price for Pettah
    dambulla_today_col = 'col_7'  # Today's price for Dambulla
    
    for row in rows:
        # Check for section markers
        if is_vegetable_section(row):
            veg_section_start = True
            continue
            
        if any('O  T  H  E  R' in str(val) for val in row.values()):
            other_section_start = True
            
        # Skip if not in vegetable section or if in other section
        if not veg_section_start or other_section_start:
            continue
            
        # Get vegetable name
        vegetable = row.get('col_0', '').strip()
        if not vegetable:
            continue
            
        # Get Pettah wholesale price (Today)
        pettah_price = row.get(pettah_today_col, '').strip()
        
        # Get Dambulla wholesale price (Today)
        dambulla_prices = row.get(dambulla_today_col, '').strip().split()
        if dambulla_prices:
            dambulla_price = dambulla_prices[-1]  # Last price is today's price
        else:
            dambulla_price = None
        
        # Format prices
        formatted_pettah = format_price(pettah_price)
        formatted_dambulla = format_price(dambulla_price)
        
        if formatted_pettah or formatted_dambulla:
            prices.append((
                vegetable,
                formatted_pettah or "N/A",
                formatted_dambulla or "N/A"
            ))
    
    return prices

def generate_report():
    # Get all unique dates from the database
    dates = collection.distinct('date')
    
    if not dates:
        print("No data found in the database!")
        return
        
    # Create reports directory if it doesn't exist
    os.makedirs('reports', exist_ok=True)
    
    # Generate report for each date
    for date in sorted(dates):
        print(f"\nProcessing data for {date}...")
        
        # Find documents for this date
        docs = list(collection.find({'date': date, 'page': 2}))
        
        if not docs:
            print(f"No data found for {date}")
            continue
            
        # Extract wholesale prices from the first table (assuming one table per page)
        prices = extract_wholesale_prices(docs[0])
        
        if not prices:
            print(f"No vegetable prices found for {date}")
            continue
        
        # Generate report text
        date_str = date.strftime('%Y-%m-%d')
        report_text = f"Today's Wholesale Vegetable Prices - {date_str}\n"
        report_text += "=" * 80 + "\n\n"
        
        # Add header
        max_veg_length = max(len(veg) for veg, _, _ in prices)
        header_format = "{:<{}} | {:>15} | {:>15}\n"
        report_text += header_format.format(
            "Vegetable",
            max_veg_length,
            "Pettah",
            "Dambulla"
        )
        report_text += "-" * max_veg_length + "-+-" + "-" * 16 + "-+-" + "-" * 15 + "\n"
        
        # Add prices
        row_format = "{:<{}} | Rs. {:>12} | Rs. {:>12}\n"
        for vegetable, pettah_price, dambulla_price in prices:
            report_text += row_format.format(
                vegetable,
                max_veg_length,
                pettah_price,
                dambulla_price
            )
        
        # Add note about the prices
        report_text += "\nNote: These are today's wholesale prices for both markets.\n"
        
        # Write to file
        filename = f"reports/vegetable_prices_{date_str}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        print(f"Generated report for {date_str}")

if __name__ == "__main__":
    generate_report()
