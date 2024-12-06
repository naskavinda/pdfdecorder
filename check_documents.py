from pymongo import MongoClient
from pprint import pprint

# Connect to MongoDB
client = MongoClient('mongodb://root:secret@localhost:27017/')
db = client['pdf_data']
collection = db['extracted_tables']

# Get and print one document to verify structure
doc = collection.find_one()
print("Sample document structure:")
pprint(doc)
