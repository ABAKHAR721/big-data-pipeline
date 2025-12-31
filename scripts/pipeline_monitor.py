import time
import requests
import subprocess
from datetime import datetime

class PipelineMonitor:
    def __init__(self):
        self.services = {
            "MongoDB": "http://mongodb:27017",
            "Flask API": "http://flask-api:5000",
            "Spark Master": "http://spark-master:8080",
            "Hadoop Namenode": "http://hadoop-namenode:9870"
        }
    
    def check_services(self):
        print(f"\n🔍 [{datetime.now()}] Checking services...")
        
        for service, url in self.services.items():
            try:
                if service == "MongoDB":
                    # Check MongoDB differently
                    result = subprocess.run(["mongo", "--host", "mongodb", "--eval", "db.runCommand('ping')"], 
                                          capture_output=True, text=True, timeout=5)
                    status = "✅ UP" if result.returncode == 0 else "❌ DOWN"
                else:
                    response = requests.get(url, timeout=5)
                    status = "✅ UP" if response.status_code == 200 else "❌ DOWN"
                
                print(f"  {service}: {status}")
                
            except Exception as e:
                print(f"  {service}: ❌ DOWN ({str(e)[:50]})")
    
    def get_pipeline_stats(self):
        try:
            response = requests.get("http://flask-api:5000/collections", timeout=5)
            if response.status_code == 200:
                data = response.json()
                print(f"\n📊 Pipeline Stats:")
                for collection, count in data.get("document_counts", {}).items():
                    print(f"  {collection}: {count} documents")
            else:
                print("📊 Pipeline Stats: Unable to fetch")
        except Exception as e:
            print(f"📊 Pipeline Stats: Error - {e}")
    
    def monitor(self):
        print("🚀 Starting Pipeline Monitor...")
        
        while True:
            self.check_services()
            self.get_pipeline_stats()
            
            print(f"\n⏰ Next check in 10 minutes...")
            time.sleep(600)  # Check every 10 minutes

if __name__ == "__main__":
    monitor = PipelineMonitor()
    monitor.monitor()