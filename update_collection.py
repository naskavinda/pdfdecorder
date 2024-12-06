from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://root:secret@localhost:27017/')
db = client['pdf_data']
collection = db['extracted_tables']

# Update all documents to add type field
result = collection.update_many(
    {},  # match all documents
    {'$set': {'type': 'vegetables'}}  # set type field to 'vegetables'
)

print(f"Modified {result.modified_count} documents")
