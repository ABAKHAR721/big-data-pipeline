#!/bin/bash

echo "🚀 Starting automatic Spark analysis service..."

while true; do
    echo "$(date): Waiting for data files..."
    
    # Check if there are JSON files to analyze
    if ls /app/data/quotes*.json 1> /dev/null 2>&1; then
        echo "$(date): Found data files, starting Spark analysis..."
        
        # Run Spark analysis with full path
        /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.sql.adaptive.enabled=false /app/spark_jobs/analyze_quotes.py
        
        echo "$(date): Spark analysis completed"
    else
        echo "$(date): No data files found yet"
    fi
    
    echo "$(date): Sleeping for 30 minutes..."
    sleep 1800  # Wait 30 minutes before next analysis
done