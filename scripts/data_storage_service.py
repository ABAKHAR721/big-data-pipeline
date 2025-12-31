import json
import os
from pymongo import MongoClient
from datetime import datetime
import glob
import time

def store_quotes_in_mongodb():
    print("🔄 Starting MongoDB storage service...")
    
    # Connect to MongoDB
    client = MongoClient('mongodb://mongodb:27017/')
    db = client.research_db
    collection = db.scraped_data
    
    while True:
        try:
            # Find all JSON files in data directory
            json_files = glob.glob('/app/data/quotes*.json')
            
            if json_files:
                print(f"📁 Found {len(json_files)} files to process")
                
                for json_file in json_files:
                    try:
                        with open(json_file, 'r', encoding='utf-8') as f:
                            quotes = json.load(f)
                        
                        # Add metadata to each quote
                        for quote in quotes:
                            quote['stored_at'] = datetime.now()
                            quote['source_file'] = os.path.basename(json_file)
                        
                        # Insert into MongoDB (update if exists)
                        for quote in quotes:
                            collection.update_one(
                                {'text': quote['text'], 'author': quote['author']},
                                {'$set': quote},
                                upsert=True
                            )
                        
                        print(f"✅ Stored {len(quotes)} quotes from {json_file} to MongoDB")
                        
                        # Also store in HDFS for long-term storage
                        store_to_hdfs(json_file)
                        
                    except Exception as e:
                        print(f"❌ Error processing {json_file}: {e}")
                
                total_docs = collection.count_documents({})
                print(f"📊 Total quotes in database: {total_docs}")
            else:
                print("📁 No new files found")
            
        except Exception as e:
            print(f"❌ Storage error: {e}")
        
        print("⏰ Sleeping for 5 minutes...")
        time.sleep(300)  # Check every 5 minutes

def store_to_hdfs(json_file):
    """HDFS storage handled by separate host script"""
    print(f"💾 HDFS: File {os.path.basename(json_file)} will be stored by host script")

if __name__ == "__main__":
    store_quotes_in_mongodb()