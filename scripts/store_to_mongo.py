import json
import os
from pymongo import MongoClient
from datetime import datetime
import glob

def store_quotes_in_mongodb():
    # Connect to MongoDB
    client = MongoClient('mongodb://mongodb:27017/')
    db = client.quotes_db
    collection = db.quotes
    
    # Find all JSON files in data directory
    json_files = glob.glob('/app/data/quotes_*.json')
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                quotes = json.load(f)
            
            # Add metadata to each quote
            for quote in quotes:
                quote['scraped_at'] = datetime.now()
                quote['source_file'] = os.path.basename(json_file)
            
            # Insert into MongoDB (update if exists)
            for quote in quotes:
                collection.update_one(
                    {'text': quote['text'], 'author': quote['author']},
                    {'$set': quote},
                    upsert=True
                )
            
            print(f"Stored {len(quotes)} quotes from {json_file}")
            
        except Exception as e:
            print(f"Error processing {json_file}: {e}")
    
    print(f"Total quotes in database: {collection.count_documents({})}")

if __name__ == "__main__":
    store_quotes_in_mongodb()