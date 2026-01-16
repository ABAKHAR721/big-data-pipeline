from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import os
from pymongo import MongoClient
from collections import Counter
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# MongoDB connection - dynamic database
client = MongoClient('mongodb://mongodb:27017/')
db = client.dynamic_data_db

@app.route('/')
def home():
    return jsonify({
        "message": "Dynamic Data API", 
        "version": "2.0",
        "supported_data_types": ["research_papers", "quotes", "generic"],
        "endpoints": [
            "/data/<collection>",
            "/stats/<collection>", 
            "/search/<collection>",
            "/collections",
            "/analytics"
        ]
    })

@app.route('/collections')
def get_collections():
    collections = db.list_collection_names()
    stats = {}
    for col in collections:
        count = db[col].count_documents({})
        # Detect data type
        sample = db[col].find_one()
        data_type = "unknown"
        if sample:
            if 'keyword' in sample and 'authors' in sample:
                data_type = "research_papers"
            elif 'text' in sample and 'author' in sample:
                data_type = "quotes"
        
        stats[col] = {
            "count": count,
            "data_type": data_type
        }
    
    return jsonify({"collections": collections, "stats": stats})

@app.route('/data/<collection>')
def get_data(collection):
    try:
        limit = int(request.args.get('limit', 100))
        skip = int(request.args.get('skip', 0))
        
        data = list(db[collection].find().skip(skip).limit(limit))
        
        # Convert ObjectId to string
        for item in data:
            if '_id' in item:
                item['_id'] = str(item['_id'])
        
        return jsonify({
            "collection": collection,
            "count": len(data),
            "total": db[collection].count_documents({}),
            "data": data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/stats/<collection>')
def get_stats(collection):
    try:
        total = db[collection].count_documents({})
        
        # Get recent data (last 24 hours)
        recent = db[collection].count_documents({
            "stored_at": {"$gte": datetime.now().replace(hour=0, minute=0, second=0)}
        })
        
        # Dynamic stats based on data type
        sample = db[collection].find_one()
        dynamic_stats = {}
        
        if sample:
            if 'keyword' in sample:  # Research papers
                keywords = db[collection].distinct('keyword')
                countries = db[collection].distinct('country')
                years = db[collection].distinct('year')
                dynamic_stats = {
                    "unique_keywords": len(keywords),
                    "unique_countries": len(countries),
                    "year_range": f"{min(years)} - {max(years)}" if years else "N/A"
                }
            elif 'author' in sample:  # Quotes
                authors = db[collection].distinct('author')
                dynamic_stats = {
                    "unique_authors": len(authors)
                }
        
        return jsonify({
            "collection": collection,
            "total_documents": total,
            "recent_documents": recent,
            "dynamic_stats": dynamic_stats,
            "last_updated": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/search/<collection>')
def search_data(collection):
    try:
        query = request.args.get('q', '')
        limit = int(request.args.get('limit', 50))
        
        if not query:
            return jsonify({"error": "Query parameter 'q' is required"}), 400
        
        # Dynamic search based on collection structure
        sample = db[collection].find_one()
        search_fields = []
        
        if sample:
            if 'title' in sample:  # Research papers
                search_fields = [
                    {"title": {"$regex": query, "$options": "i"}},
                    {"authors": {"$regex": query, "$options": "i"}},
                    {"keyword": {"$regex": query, "$options": "i"}},
                    {"abstract": {"$regex": query, "$options": "i"}}
                ]
            elif 'text' in sample:  # Quotes
                search_fields = [
                    {"text": {"$regex": query, "$options": "i"}},
                    {"author": {"$regex": query, "$options": "i"}}
                ]
        
        if not search_fields:
            return jsonify({"error": "No searchable fields found"}), 400
        
        results = list(db[collection].find({"$or": search_fields}).limit(limit))
        
        # Convert ObjectId to string
        for item in results:
            if '_id' in item:
                item['_id'] = str(item['_id'])
        
        return jsonify({
            "query": query,
            "collection": collection,
            "results": results,
            "count": len(results)
        })
    
    except Exception as e:
        import traceback
        return jsonify({
            "error": str(e),
            "traceback": traceback.format_exc(),
            "query": query,
            "collection": collection
        }), 500

@app.route('/analytics')
def get_analytics():
    """Load Spark analysis results"""
    try:
        analysis_file = "/app/data/analysis_results.json"
        if os.path.exists(analysis_file):
            with open(analysis_file, 'r') as f:
                analytics_data = json.load(f)
            return jsonify(analytics_data)
        else:
            return jsonify({
                "message": "No analytics data available. Run Spark analysis first.",
                "status": "no_data"
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)