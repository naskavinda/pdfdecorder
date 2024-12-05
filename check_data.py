from pymongo import MongoClient
import json

# MongoDB connection
client = MongoClient('mongodb://root:secret@localhost:27017/')
db = client['pdf_data']
collection = db['extracted_tables']

# Get one document to see its structure
doc = collection.find_one()
if doc:
    # Convert ObjectId to string for JSON serialization
    doc['_id'] = str(doc['_id'])
    doc['date'] = str(doc['date'])
    
    print("Sample document structure:")
    print(json.dumps(doc, indent=2))
    
    print("\nColumns in the table:")
    print(doc['data']['columns'])
else:
    print("No documents found in the collection")
