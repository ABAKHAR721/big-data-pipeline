import json
import os
from pymongo import MongoClient
from datetime import datetime
import glob
import time

def store_data_in_mongodb():
    print("🔄 Starting dynamic MongoDB storage service...")
    
    # Connect to MongoDB
    client = MongoClient('mongodb://mongodb:27017/')
    db = client.dynamic_data_db
    collection = db.raw_data
    
    while True:
        try:
            # Find all JSON files - support multiple data types
            json_files = glob.glob('/app/data/research*.json') + glob.glob('/app/data/quotes*.json')
            
            if json_files:
                print(f"📁 Found {len(json_files)} files to process")
                
                for json_file in json_files:
                    try:
                        with open(json_file, 'r', encoding='utf-8') as f:
                            data_items = json.load(f)
                        
                        # Add metadata to each item
                        for item in data_items:
                            item['stored_at'] = datetime.now()
                            item['source_file'] = os.path.basename(json_file)
                        
                        # Dynamic insertion based on data type
                        for item in data_items:
                            # Determine unique fields for deduplication
                            if 'title' in item:  # Research papers
                                collection.update_one(
                                    {'title': item['title'], 'authors': item.get('authors', '')},
                                    {'$set': item},
                                    upsert=True
                                )
                            elif 'text' in item:  # Quotes
                                collection.update_one(
                                    {'text': item['text'], 'author': item.get('author', '')},
                                    {'$set': item},
                                    upsert=True
                                )
                            else:  # Generic data
                                collection.insert_one(item)
                        
                        data_type = "research papers" if 'research' in json_file else "quotes"
                        print(f"✅ Stored {len(data_items)} {data_type} from {json_file} to MongoDB")
                        
                        # Also store in HDFS for long-term storage
                        store_to_hdfs(json_file)
                        
                    except Exception as e:
                        print(f"❌ Error processing {json_file}: {e}")
                
                total_docs = collection.count_documents({})
                print(f"📊 Total documents in database: {total_docs}")
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
    store_data_in_mongodb()