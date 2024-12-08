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
        f.write(f"Price Report for {report_date}\n")
        f.write("=" * 60 + "\n\n")
        
        if 'data' in doc:
            # Group items by type
            items_by_type = {}
            for item in doc['data']:
                item_type = item['type']
                if item_type not in items_by_type:
                    items_by_type[item_type] = []
                items_by_type[item_type].append(item)
            
            # Process each type
            for item_type, items in items_by_type.items():
                # Write section header
                f.write(f"\n{item_type.upper()}\n")
                f.write("=" * 60 + "\n\n")
                
                # Check if it's rice data
                is_rice = item_type == 'rice'
                
                # Write wholesale prices
                f.write("Wholesale Prices (Rs./kg):\n")
                f.write("-" * 80 + "\n")
                if is_rice:
                    f.write(f"{'Item':<25} {'Pettah':>15} {'Marandagahamula':>20}\n")
                else:
                    f.write(f"{'Item':<25} {'Pettah':>15} {'Dambulla':>15}\n")
                f.write("-" * 80 + "\n")
                
                for row in items:
                    item = row['item']
                    pettah_wholesale = format_price(row['pettah_wholesale']['today'])
                    if is_rice:
                        marandagahamula_wholesale = format_price(row['marandagahamula_wholesale']['today'])
                        f.write(f"{item:<25} {pettah_wholesale:>15} {marandagahamula_wholesale:>20}\n")
                    else:
                        dambulla_wholesale = format_price(row['dambulla_wholesale']['today'])
                        f.write(f"{item:<25} {pettah_wholesale:>15} {dambulla_wholesale:>15}\n")
                
                # Write retail prices
                f.write("\n\nRetail Prices (Rs./kg):\n")
                f.write("-" * 80 + "\n")
                f.write(f"{'Item':<25} {'Pettah':>15} {'Dambulla':>15} {'Narahenpita':>15}\n")
                f.write("-" * 80 + "\n")
                
                for row in items:
                    item = row['item']
                    pettah_retail = format_price(row['pettah_retail']['today'])
                    dambulla_retail = format_price(row['dambulla_retail']['today'])
                    narahenpita_retail = format_price(row['narahenpita_retail']['today'])
                    
                    f.write(f"{item:<25} {pettah_retail:>15} {dambulla_retail:>15} {narahenpita_retail:>15}\n")
                
                f.write("\n" + "-" * 80 + "\n")
        
        f.write("\nNote: All prices are in Sri Lankan Rupees (Rs.)\n")
    
    print(f"Report generated: {report_file}")

def generate_report():
    """Generate reports for all documents in the database"""
    # Create reports directory if it doesn't exist
    os.makedirs('reports', exist_ok=True)
    
    # Group documents by date
    documents_by_date = {}
    for doc in collection.find():
        date = doc.get('date')
        if date not in documents_by_date:
            documents_by_date[date] = {
                'date': date,
                'data': []
            }
        if 'data' in doc:
            documents_by_date[date]['data'].extend(doc['data'])
    
    # Generate a report for each day
    for date, combined_doc in documents_by_date.items():
        if isinstance(date, datetime):
            report_date = date.strftime('%Y-%m-%d')
        else:
            report_date = str(date)
        report_file = f'reports/price_report_{report_date}.txt'
        generate_single_report(combined_doc, report_file)

def display_todays_prices(data):
    """Display today's wholesale and retail prices for all cities, categorized by type"""
    # Group items by type
    items_by_type = {}
    for item in data:
        item_type = item['type']
        if item_type not in items_by_type:
            items_by_type[item_type] = []
        items_by_type[item_type].append(item)
    
    # Display prices for each type
    for item_type, items in items_by_type.items():
        print(f"\n{item_type.upper()} - Today's Prices Report")
        print("=" * 80)
        print(f"{'Item':<15} {'Location':<15} {'Wholesale':<15} {'Retail':<15}")
        print("-" * 80)
        
        for item in items:
            # Pettah prices
            print(f"{item['item']:<15} {'Pettah':<15} {format_price(item['pettah_wholesale']['today']):<15} {format_price(item['pettah_retail']['today']):<15}")
            
            # Check if it's rice data (using marandagahamula) or other data (using dambulla)
            if 'marandagahamula_wholesale' in item:
                print(f"{'':<15} {'Marandagahamula':<15} {format_price(item['marandagahamula_wholesale']['today']):<15} {'N/A':<15}")
                print(f"{'':<15} {'Dambulla':<15} {'N/A':<15} {format_price(item['dambulla_retail']['today']):<15}")
            else:
                print(f"{'':<15} {'Dambulla':<15} {format_price(item['dambulla_wholesale']['today']):<15} {format_price(item['dambulla_retail']['today']):<15}")
            
            # Narahenpita retail only
            print(f"{'':<15} {'Narahenpita':<15} {'N/A':<15} {format_price(item['narahenpita_retail']['today']):<15}")
            print("-" * 80)

if __name__ == '__main__':
    # Get all documents from the most recent date
    latest_date = collection.find_one({}, sort=[("date", -1)])['date']
    latest_docs = collection.find({"date": latest_date})
    
    print(f"Found data for date: {latest_date}")
    
    # Use a dictionary to store unique items by their type and name
    unique_items = {}
    for doc in latest_docs:
        if 'data' in doc:
            for item in doc['data']:
                item_key = (item['type'], item['item'])
                # Only keep the item if it's not already present or if it has a more recent timestamp
                if item_key not in unique_items or item['timestamp'] > unique_items[item_key]['timestamp']:
                    unique_items[item_key] = item
    
    # Convert back to list
    all_data = list(unique_items.values())
    
    # Print unique types found
    types = set(item['type'] for item in all_data)
    print(f"Found item types: {types}")
    
    if all_data:
        display_todays_prices(all_data)
    else:
        print("No data found")
    generate_report()
