#!/bin/bash

echo "Starting periodic scraping..."

while true; do
    timestamp=$(date +%Y%m%d_%H%M%S)
    echo "$(date): Running arXiv research crawler..."
    cd /app
    
    # Use arXiv spider for research papers
    scrapy crawl arxiv_spider -o data/research_${timestamp}.json
    
    # Also update the main file
    cp data/research_${timestamp}.json data/research_data.json
    
    echo "$(date): Research scraping completed. Data saved to research_${timestamp}.json"
    echo "$(date): Sleeping for 1 hour..."
    sleep 3600  # Sleep for 1 hour
done