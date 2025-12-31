#!/bin/bash

echo "Starting periodic scraping..."

while true; do
    timestamp=$(date +%Y%m%d_%H%M%S)
    echo "$(date): Running scrapy crawler..."
    cd /app
    
    # Create new file with timestamp to avoid JSON format issues
    scrapy crawl example -o data/quotes_${timestamp}.json
    
    # Also update the main quotes.json file (overwrite, don't append)
    cp data/quotes_${timestamp}.json data/quotes.json
    
    echo "$(date): Scraping completed. Data saved to quotes_${timestamp}.json"
    echo "$(date): Sleeping for 1 hour..."
    sleep 3600  # Sleep for 1 hour
done