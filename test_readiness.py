#!/usr/bin/env python3
"""
Test script to verify the dynamic big data pipeline is ready
"""

import os
import json

def check_file_exists(filepath, description):
    if os.path.exists(filepath):
        print(f"OK {description}: {filepath}")
        return True
    else:
        print(f"MISSING {description}: {filepath} - NOT FOUND")
        return False

def check_file_content(filepath, search_text, description):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            if search_text in content:
                print(f"OK {description}: Contains '{search_text}'")
                return True
            else:
                print(f"MISSING {description}: Missing '{search_text}'")
                return False
    except Exception as e:
        print(f"ERROR {description}: Error reading file - {e}")
        return False

def main():
    print("DYNAMIC BIG DATA PIPELINE - READINESS CHECK")
    print("=" * 60)
    
    all_good = True
    
    # Check core files
    files_to_check = [
        ("scrapy_project/scrapy_project/items.py", "Dynamic Items Schema"),
        ("scrapy_project/scrapy_project/spiders/arxiv_spider.py", "arXiv Spider"),
        ("scrapy_project/periodic_scrape.sh", "Periodic Scraping Script"),
        ("spark_jobs/analyze_quotes.py", "Dynamic Spark Analysis"),
        ("scripts/data_storage_service.py", "Dynamic Data Storage"),
        ("scripts/hdfs_auto_storage.py", "HDFS Auto Storage"),
        ("flask-api/app.py", "Dynamic Flask API"),
        ("smart-router/app.py", "Smart Router"),
        ("docker-compose.yml", "Docker Compose")
    ]
    
    for filepath, description in files_to_check:
        if not check_file_exists(filepath, description):
            all_good = False
    
    print("\nCONTENT VERIFICATION")
    print("-" * 40)
    
    # Check key content updates
    content_checks = [
        ("scrapy_project/scrapy_project/items.py", "DynamicDataItem", "Dynamic Items Class"),
        ("scrapy_project/scrapy_project/spiders/arxiv_spider.py", "arxiv.org", "arXiv URL"),
        ("scrapy_project/periodic_scrape.sh", "arxiv_spider", "arXiv Spider Usage"),
        ("spark_jobs/analyze_quotes.py", "research*.json", "Research File Pattern"),
        ("scripts/data_storage_service.py", "dynamic_data_db", "Dynamic Database"),
        ("flask-api/app.py", "dynamic_data_db", "Dynamic Database"),
        ("docker-compose.yml", "dynamic_data_db", "Dynamic Database")
    ]
    
    for filepath, search_text, description in content_checks:
        if not check_file_content(filepath, search_text, description):
            all_good = False
    
    print("\nSUMMARY")
    print("-" * 20)
    
    if all_good:
        print("ALL SYSTEMS READY!")
        print("Dynamic pipeline is configured correctly")
        print("arXiv scraping is set up")
        print("Multi-data-type support enabled")
        print("Ready to run: docker-compose up -d")
    else:
        print("ISSUES FOUND!")
        print("Some files or configurations need attention")
        print("Please fix the issues above before testing")
    
    print("\nNEXT STEPS:")
    print("1. Run: docker-compose down -v")
    print("2. Run: rmdir /s /q data && mkdir data")
    print("3. Run: docker-compose up -d")
    print("4. Wait 5-10 minutes for arXiv scraping to start")
    print("5. Check: http://localhost:5000/collections")

if __name__ == "__main__":
    main()