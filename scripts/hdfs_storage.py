#!/usr/bin/env python3
import os
import subprocess
import glob

def store_files_to_hdfs():
    """Store JSON files directly to HDFS from host"""
    try:
        json_files = glob.glob("data/quotes*.json")
        
        for json_file in json_files:
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
            else:
                print(f"HDFS error for {filename}: {result.stderr}")
                
    except Exception as e:
        print(f"HDFS storage error: {e}")

if __name__ == "__main__":
    store_files_to_hdfs()