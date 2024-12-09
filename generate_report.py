from pymongo import MongoClient
from datetime import datetime, timedelta
import os

# MongoDB connection
client = MongoClient('mongodb://root:secret@localhost:27017/')
db = client['pdf_data']

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

def save_to_mongodb(doc):
    """Save data to MongoDB in item-specific collections with dates as keys"""
    if 'data' in doc:
        date = doc.get('date')
        # Convert date to YYYYMMDD format for the key
        date_key = date.strftime('%Y%m%d') if isinstance(date, datetime) else date.replace('-', '')
        
        # Group items by type
        items_by_type = {}
        for item in doc['data']:
            item_type = item['type']
            if item_type not in items_by_type:
                items_by_type[item_type] = []
            items_by_type[item_type].append(item)
        
        # Save each type to its own collection
        for item_type, items in items_by_type.items():
            collection_name = f"{item_type}_prices"
            collection = db[collection_name]
            
            # Process each item
            for item_data in items:
                # Create the price data structure
                price_data = {
                    'date': date,
                    'wholesale': {},
                    'retail': {}
                }
                
                # Add wholesale prices based on item type
                if item_type == 'fish':
                    price_data['wholesale'].update({
                        'peliyagoda': item_data.get('peliyagoda_wholesale', {}).get('today'),
                        'negombo': item_data.get('negombo_wholesale', {}).get('today')
                    })
                    price_data['retail'].update({
                        'pettah': item_data.get('pettah_retail', {}).get('today'),
                        'negombo': item_data.get('negombo_retail', {}).get('today'),
                        'narahenpita': item_data.get('narahenpita_retail', {}).get('today')
                    })
                elif item_type == 'rice':
                    price_data['wholesale'].update({
                        'pettah': item_data.get('pettah_wholesale', {}).get('today'),
                        'marandagahamula': item_data.get('marandagahamula_wholesale', {}).get('today')
                    })
                    price_data['retail'].update({
                        'pettah': item_data.get('pettah_retail', {}).get('today'),
                        'dambulla': item_data.get('dambulla_retail', {}).get('today'),
                        'narahenpita': item_data.get('narahenpita_retail', {}).get('today')
                    })
                else:
                    price_data['wholesale'].update({
                        'pettah': item_data.get('pettah_wholesale', {}).get('today'),
                        'dambulla': item_data.get('dambulla_wholesale', {}).get('today')
                    })
                    price_data['retail'].update({
                        'pettah': item_data.get('pettah_retail', {}).get('today'),
                        'dambulla': item_data.get('dambulla_retail', {}).get('today'),
                        'narahenpita': item_data.get('narahenpita_retail', {}).get('today')
                    })
                
                # Update the document for this item
                update_data = {
                    '$set': {
                        'item': item_data['item'],
                        'type': item_data['type'],
                        date_key: price_data
                    }
                }
                
                # Insert or update using item name as the identifier
                collection.update_one(
                    {'item': item_data['item']},
                    update_data,
                    upsert=True
                )

def generate_single_report(doc, report_file):
    """Generate a report for a single day and save to MongoDB"""
    # Save to MongoDB first
    save_to_mongodb(doc)
    
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
                
                # Check data type
                is_rice = item_type == 'rice'
                is_fish = item_type == 'fish'
                
                # Write wholesale prices
                f.write("Wholesale Prices (Rs./kg):\n")
                f.write("-" * 80 + "\n")
                if is_rice:
                    f.write(f"{'Item':<25} {'Pettah':>15} {'Marandagahamula':>20}\n")
                elif is_fish:
                    f.write(f"{'Item':<25} {'Peliyagoda':>15} {'Negombo':>15}\n")
                else:
                    f.write(f"{'Item':<25} {'Pettah':>15} {'Dambulla':>15}\n")
                f.write("-" * 80 + "\n")
                
                for row in items:
                    item = row['item']
                    if is_fish:
                        peliyagoda_wholesale = format_price(row.get('peliyagoda_wholesale', {}).get('today'))
                        negombo_wholesale = format_price(row.get('negombo_wholesale', {}).get('today'))
                        f.write(f"{item:<25} {peliyagoda_wholesale:>15} {negombo_wholesale:>15}\n")
                    elif is_rice:
                        pettah_wholesale = format_price(row.get('pettah_wholesale', {}).get('today'))
                        marandagahamula_wholesale = format_price(row.get('marandagahamula_wholesale', {}).get('today'))
                        f.write(f"{item:<25} {pettah_wholesale:>15} {marandagahamula_wholesale:>20}\n")
                    else:
                        pettah_wholesale = format_price(row.get('pettah_wholesale', {}).get('today'))
                        dambulla_wholesale = format_price(row.get('dambulla_wholesale', {}).get('today'))
                        f.write(f"{item:<25} {pettah_wholesale:>15} {dambulla_wholesale:>15}\n")
                
                # Write retail prices
                f.write("\n\nRetail Prices (Rs./kg):\n")
                f.write("-" * 80 + "\n")
                if is_fish:
                    f.write(f"{'Item':<25} {'Pettah':>15} {'Negombo':>15} {'Narahenpita':>15}\n")
                else:
                    f.write(f"{'Item':<25} {'Pettah':>15} {'Dambulla':>15} {'Narahenpita':>15}\n")
                f.write("-" * 80 + "\n")
                
                for row in items:
                    item = row['item']
                    if is_fish:
                        pettah_retail = format_price(row.get('pettah_retail', {}).get('today'))
                        negombo_retail = format_price(row.get('negombo_retail', {}).get('today'))
                        narahenpita_retail = format_price(row.get('narahenpita_retail', {}).get('today'))
                        f.write(f"{item:<25} {pettah_retail:>15} {negombo_retail:>15} {narahenpita_retail:>15}\n")
                    else:
                        pettah_retail = format_price(row.get('pettah_retail', {}).get('today'))
                        dambulla_retail = format_price(row.get('dambulla_retail', {}).get('today'))
                        narahenpita_retail = format_price(row.get('narahenpita_retail', {}).get('today'))
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
    for doc in db['extracted_tables'].find():
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
    if not data:
        print("No data available")
        return

    for doc in data:
        if 'data' not in doc:
            continue

        print(f"\nPrices for {doc['date']}")
        print("=" * 60)

        # Group items by type
        items_by_type = {}
        for item in doc['data']:
            item_type = item['type']
            if item_type not in items_by_type:
                items_by_type[item_type] = []
            items_by_type[item_type].append(item)

        # Display prices for each type
        for item_type, items in items_by_type.items():
            print(f"\n{item_type.upper()}")
            print("-" * 60)
            print(f"{'Item':<15} {'Location':<15} {'Wholesale':<15} {'Retail':<15}")
            print("-" * 60)

            for item in items:
                if item_type == 'fish':
                    # Print Peliyagoda prices
                    wholesale_price = item.get('peliyagoda_wholesale', {}).get('today')
                    print(f"{item['item']:<15} {'Peliyagoda':<15} {format_price(wholesale_price):<15} {'N/A':<15}")
                    
                    # Print Negombo prices
                    wholesale_price = item.get('negombo_wholesale', {}).get('today')
                    retail_price = item.get('negombo_retail', {}).get('today')
                    print(f"{'':<15} {'Negombo':<15} {format_price(wholesale_price):<15} {format_price(retail_price):<15}")
                    
                    # Print Narahenpita retail only
                    retail_price = item.get('narahenpita_retail', {}).get('today')
                    print(f"{'':<15} {'Narahenpita':<15} {'N/A':<15} {format_price(retail_price):<15}")
                
                elif item_type == 'rice':
                    # Print Pettah prices
                    wholesale_price = item.get('pettah_wholesale', {}).get('today')
                    retail_price = item.get('pettah_retail', {}).get('today')
                    print(f"{item['item']:<15} {'Pettah':<15} {format_price(wholesale_price):<15} {format_price(retail_price):<15}")
                    
                    # Print Marandagahamula wholesale only
                    wholesale_price = item.get('marandagahamula_wholesale', {}).get('today')
                    print(f"{'':<15} {'Marandagah.':<15} {format_price(wholesale_price):<15} {'N/A':<15}")
                    
                    # Print Narahenpita retail only
                    retail_price = item.get('narahenpita_retail', {}).get('today')
                    print(f"{'':<15} {'Narahenpita':<15} {'N/A':<15} {format_price(retail_price):<15}")
                
                else:
                    # Print Pettah prices
                    wholesale_price = item.get('pettah_wholesale', {}).get('today')
                    retail_price = item.get('pettah_retail', {}).get('today')
                    print(f"{item['item']:<15} {'Pettah':<15} {format_price(wholesale_price):<15} {format_price(retail_price):<15}")
                    
                    # Print Dambulla prices
                    wholesale_price = item.get('dambulla_wholesale', {}).get('today')
                    retail_price = item.get('dambulla_retail', {}).get('today')
                    print(f"{'':<15} {'Dambulla':<15} {format_price(wholesale_price):<15} {format_price(retail_price):<15}")
                    
                    # Print Narahenpita retail only
                    retail_price = item.get('narahenpita_retail', {}).get('today')
                    print(f"{'':<15} {'Narahenpita':<15} {'N/A':<15} {format_price(retail_price):<15}")
                
                print("-" * 60)

if __name__ == '__main__':
    # Get all documents from the most recent date
    latest_date = db['extracted_tables'].find_one({}, sort=[("date", -1)])['date']
    latest_docs = db['extracted_tables'].find({"date": latest_date})
    
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
        display_todays_prices([{'date': latest_date, 'data': all_data}])
    else:
        print("No data found")
    generate_report()
