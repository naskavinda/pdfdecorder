from pymongo import MongoClient
from datetime import datetime, timedelta
import os

# MongoDB connection
client = MongoClient('mongodb://root:secret@localhost:27017/')
db = client['pdf_data']
collection = db['extracted_tables']

def format_price(price):
    """Format price to proper format"""
    try:
        if price is None:
            return 'N/A'
        return f"{float(price):,.2f}"
    except (ValueError, TypeError):
        return 'N/A'

def calc_change(today, yesterday):
    """Calculate price change percentage"""
    try:
        if today is not None and yesterday is not None and yesterday != 0:
            return ((today - yesterday) / yesterday) * 100
        return None
    except (ValueError, TypeError):
        return None

def format_change(change):
    """Format price change percentage"""
    try:
        if change is None:
            return ''
        return f"({change:+.1f}%)" if abs(change) > 0 else ''
    except (ValueError, TypeError):
        return ''

def generate_single_report(doc, report_file):
    """Generate a report for a single day"""
    with open(report_file, 'w') as f:
        report_date = doc.get('date', 'Unknown Date')
        
        # Write header
        f.write(f"Vegetable Price Report for {report_date}\n")
        f.write("=" * 60 + "\n\n")
        
        if 'data' in doc:
            # Write wholesale prices
            f.write("Wholesale Prices (Rs./kg):\n")
            f.write("-" * 60 + "\n")
            f.write(f"{'Item':<25} {'Pettah':>15} {'Dambulla':>15}\n")
            f.write("-" * 60 + "\n")
            
            for row in doc['data']:
                item = row['item']
                pettah_wholesale = format_price(row['pettah_wholesale']['today'])
                dambulla_wholesale = format_price(row['dambulla_wholesale']['today'])
                
                f.write(f"{item:<25} {pettah_wholesale:>15} {dambulla_wholesale:>15}\n")
            
            # Write retail prices
            f.write("\n\nRetail Prices (Rs./kg):\n")
            f.write("-" * 60 + "\n")
            f.write(f"{'Item':<25} {'Pettah':>15} {'Dambulla':>15} {'Narahenpita':>15}\n")
            f.write("-" * 80 + "\n")
            
            for row in doc['data']:
                item = row['item']
                pettah_retail = format_price(row['pettah_retail']['today'])
                dambulla_retail = format_price(row['dambulla_retail']['today'])
                narahenpita_retail = format_price(row['narahenpita_retail']['today'])
                
                f.write(f"{item:<25} {pettah_retail:>15} {dambulla_retail:>15} {narahenpita_retail:>15}\n")
        
        f.write("\nNote: All prices are in Sri Lankan Rupees (Rs.)\n")
    
    print(f"Report generated: {report_file}")

def generate_report():
    """Generate reports for all documents in the database"""
    # Get all documents from MongoDB
    documents = list(collection.find())
    
    # Create reports directory if it doesn't exist
    os.makedirs('reports', exist_ok=True)
    
    # Generate a report for each day
    for doc in documents:
        report_date = doc.get('date', 'Unknown Date')
        if isinstance(report_date, datetime):
            report_date = report_date.strftime('%Y-%m-%d')
        report_file = f'reports/price_report_{report_date}.txt'
        generate_single_report(doc, report_file)

if __name__ == '__main__':
    generate_report()
