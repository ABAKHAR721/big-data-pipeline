#!/usr/bin/env python3
import os
import subprocess
import glob
import time

def store_files_to_hdfs():
    """Store JSON files to HDFS automatically"""
    processed_files = set()
    
    while True:
        try:
            json_files = glob.glob("/app/data/research*.json") + glob.glob("/app/data/quotes*.json")
            new_files = [f for f in json_files if f not in processed_files]
            
            if new_files:
                print(f"Found {len(new_files)} new files to store in HDFS")
                
                for json_file in new_files:
                    filename = os.path.basename(json_file)
                    
                    # Create HDFS directory
                    subprocess.run([
                        "docker", "exec", "hadoop-namenode", 
                        "hdfs", "dfs", "-mkdir", "-p", "/research_data"
                    ], capture_output=True)
                    
                    # Copy file to HDFS
                    result = subprocess.run([
                        "docker", "exec", "hadoop-namenode",
                        "hdfs", "dfs", "-put", "-f", f"/app/data/{filename}", f"/research_data/{filename}"
                    ], capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        print(f"Stored {filename} to HDFS")
                        processed_files.add(json_file)
                    else:
                        print(f"HDFS error for {filename}")
            else:
                print("No new files to store in HDFS")
                
        except Exception as e:
            print(f"HDFS storage error: {e}")
        
        print("Sleeping for 1 minute...")
        time.sleep(60)

if __name__ == "__main__":
    print("Starting automated HDFS storage service...")
    store_files_to_hdfs()