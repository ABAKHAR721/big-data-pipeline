from flask import Flask, jsonify, request
import json
import os
from pymongo import MongoClient
from collections import Counter
from datetime import datetime

app = Flask(__name__)

# MongoDB connection
client = MongoClient('mongodb://mongodb:27017/')
db = client.research_db

@app.route('/')
def home():
    return jsonify({
        "message": "Research Data API", 
        "endpoints": [
            "/data/<collection>",
            "/stats/<collection>", 
            "/search/<collection>",
            "/collections"
        ]
    })

@app.route('/collections')
def get_collections():
    collections = db.list_collection_names()
    stats = {}
    for col in collections:
        stats[col] = db[col].count_documents({})
    return jsonify({"collections": collections, "document_counts": stats})

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
        
        return jsonify({
            "collection": collection,
            "total_documents": total,
            "recent_documents": recent,
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
        
        # Simple search
        results = list(db[collection].find({
            "$or": [
                {"text": {"$regex": query, "$options": "i"}},
                {"author": {"$regex": query, "$options": "i"}}
            ]
        }).limit(limit))
        
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)