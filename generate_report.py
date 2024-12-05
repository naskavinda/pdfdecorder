from pymongo import MongoClient
from datetime import datetime, timedelta
import os

# MongoDB connection
client = MongoClient('mongodb://root:secret@localhost:27017/')
db = client['pdf_data']
collection = db['extracted_tables']

def format_price(price_str):
    """Format price string to proper format"""
    try:
        if isinstance(price_str, str):
            price_str = price_str.replace(',', '')
            if price_str.lower() == 'n.a':
                return 'N/A'
        price_float = float(price_str)
        return f"{price_float:,.2f}"
    except (ValueError, TypeError):
        return 'N/A'

def generate_single_report(doc, report_file):
    """Generate a report for a single day"""
    with open(report_file, 'w') as f:
        report_date = doc['date'].date()
        f.write(f"Wholesale Vegetable Price Report for {report_date}\n")
        f.write("=" * 80 + "\n\n")
        
        if 'data' in doc and 'rows' in doc['data']:
            # Write wholesale prices
            f.write("Wholesale Prices (Rs./kg):\n")
            f.write("-" * 80 + "\n")
            f.write(f"{'Item':<20} {'Pettah':<30} {'Dambulla':<30}\n")
            f.write(f"{'':20} {'Yesterday':>14} {'Today':>15} {'Yesterday':>14} {'Today':>15}\n")
            f.write("-" * 80 + "\n")
            
            for row in doc['data']['rows']:
                item = row['item']
                pettah = row['wholesale']['pettah']
                dambulla = row['wholesale']['dambulla']
                
                f.write(f"{item:<20} {format_price(pettah['yesterday']):>14} {format_price(pettah['today']):>15} "
                       f"{format_price(dambulla['yesterday']):>14} {format_price(dambulla['today']):>15}\n")
            
            # Write retail prices
            f.write("\n\nRetail Prices (Rs./kg):\n")
            f.write("-" * 80 + "\n")
            f.write(f"{'Item':<20} {'Pettah':<30} {'Dambulla':<30}\n")
            f.write(f"{'':20} {'Yesterday':>14} {'Today':>15} {'Yesterday':>14} {'Today':>15}\n")
            f.write("-" * 80 + "\n")
            
            for row in doc['data']['rows']:
                item = row['item']
                pettah = row['retail']['pettah']
                dambulla = row['retail']['dambulla']
                
                f.write(f"{item:<20} {format_price(pettah['yesterday']):>14} {format_price(pettah['today']):>15} "
                       f"{format_price(dambulla['yesterday']):>14} {format_price(dambulla['today']):>15}\n")
            
            # Write Narahenpita retail prices
            f.write("\n\nNarahenpita Retail Prices (Rs./kg):\n")
            f.write("-" * 50 + "\n")
            f.write(f"{'Item':<20} {'Yesterday':>14} {'Today':>15}\n")
            f.write("-" * 50 + "\n")
            
            for row in doc['data']['rows']:
                item = row['item']
                narahenpita = row['retail']['narahenpita']
                
                f.write(f"{item:<20} {format_price(narahenpita['yesterday']):>14} {format_price(narahenpita['today']):>15}\n")
        
        f.write("\nNote: All prices are in Sri Lankan Rupees (Rs.)\n")
    
    print(f"Report generated: {report_file}")

def generate_report():
    # Create report directory if it doesn't exist
    os.makedirs('reports', exist_ok=True)
    
    # Get all documents sorted by date
    documents = collection.find().sort('date', 1)
    
    # Generate a report for each day
    for doc in documents:
        report_date = doc['date'].date()
        report_file = f'reports/price_report_{report_date}.txt'
        generate_single_report(doc, report_file)

if __name__ == "__main__":
    generate_report()
