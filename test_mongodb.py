from pymongo import MongoClient
import sys

def test_mongodb_connection():
    try:
        # Connect to MongoDB
        client = MongoClient(
            "mongodb+srv://supun2kavinda:j5FyzeeBkNndXVqN@cluster0.lsdvd.mongodb.net/data-visualizer"
        )
        
        # Test the connection by listing database names
        dbs = client.list_database_names()
        print("Successfully connected to MongoDB!")
        print("Available databases:", dbs)
        
        # Test specific database and collection
        db = client["data-visualizer"]
        collection = db["row_data"]
        
        # Count documents in collection
        doc_count = collection.count_documents({})
        print(f"Number of documents in row_data collection: {doc_count}")
        
        client.close()
        return True
        
    except Exception as e:
        print("Error connecting to MongoDB:", str(e))
        return False

if __name__ == "__main__":
    test_mongodb_connection()
