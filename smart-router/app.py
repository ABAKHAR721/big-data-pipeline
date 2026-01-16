from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import os
from pymongo import MongoClient
import subprocess
import time
from datetime import datetime
from collections import Counter

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# MongoDB connection
client = MongoClient('mongodb://mongodb:27017/')
db = client.dynamic_data_db

class SmartDataRouter:
    def __init__(self):
        self.mongo_threshold = 10000  # Si > 10K documents, utiliser HDFS
        self.complex_query_threshold = 5  # Si > 5 critères, utiliser Spark+HDFS
        
    def should_use_hdfs(self, collection, query_params):
        """Décide si utiliser HDFS ou MongoDB selon la charge"""
        
        # 1. Vérifier la taille des données
        total_docs = db[collection].count_documents({})
        if total_docs > self.mongo_threshold:
            return True, f"Large dataset ({total_docs} docs) - Using HDFS+Spark"
        
        # 2. Vérifier la complexité de la requête
        query_complexity = len(query_params)
        if query_complexity > self.complex_query_threshold:
            return True, f"Complex query ({query_complexity} params) - Using HDFS+Spark"
        
        # 3. Vérifier si c'est une requête d'analyse massive
        if 'analytics' in query_params or 'aggregate' in query_params:
            return True, "Analytics query - Using HDFS+Spark"
        
        return False, f"Simple query ({total_docs} docs) - Using MongoDB"
    
    def query_hdfs_with_spark(self, collection, query_params):
        """Execute query on HDFS with comprehensive filtering"""
        try:
            query = query_params.get('q', '')
            keyword_filter = query_params.get('keyword', '')
            country_filter = query_params.get('country', '')
            city_filter = query_params.get('city', '')
            year_filter = query_params.get('year', '')
            affiliation_filter = query_params.get('affiliation', '')
            limit = int(query_params.get('limit', 100))
            
            # Read JSON files from shared volume
            all_data = []
            import glob
            json_files = glob.glob('/app/data/research*.json') + glob.glob('/app/data/quotes*.json')
            
            for json_file in json_files:
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            all_data.extend(data)
                        else:
                            all_data.append(data)
                except Exception as e:
                    continue
            
            # Apply all filters (case-insensitive)
            filtered_data = all_data
            
            # General text search
            if query:
                filtered_data = [
                    item for item in filtered_data
                    if query.lower() in str(item.get('text', '')).lower() 
                    or query.lower() in str(item.get('author', '')).lower()
                    or query.lower() in str(item.get('title', '')).lower()
                    or query.lower() in str(item.get('authors', '')).lower()
                    or query.lower() in str(item.get('keyword', '')).lower()
                    or query.lower() in str(item.get('abstract', '')).lower()
                    or query.lower() in str(item.get('country', '')).lower()
                    or query.lower() in str(item.get('city', '')).lower()
                    or query.lower() in str(item.get('affiliation', '')).lower()
                    or query.lower() in str(item.get('year', '')).lower()
                ]
            
            # Specific field filters
            if keyword_filter:
                filtered_data = [
                    item for item in filtered_data
                    if keyword_filter.lower() in str(item.get('keyword', '')).lower()
                ]
            
            if country_filter:
                filtered_data = [
                    item for item in filtered_data
                    if country_filter.lower() in str(item.get('country', '')).lower()
                ]
            
            if city_filter:
                filtered_data = [
                    item for item in filtered_data
                    if city_filter.lower() in str(item.get('city', '')).lower()
                ]
            
            if year_filter:
                filtered_data = [
                    item for item in filtered_data
                    if year_filter.lower() in str(item.get('year', '')).lower()
                ]
            
            if affiliation_filter:
                filtered_data = [
                    item for item in filtered_data
                    if affiliation_filter.lower() in str(item.get('affiliation', '')).lower()
                ]
            
            # Analytics processing
            if 'analytics' in query_params:
                from collections import Counter
                authors = [item.get('author', 'Unknown') for item in filtered_data]
                author_counts = Counter(authors)
                for item in filtered_data:
                    item['author_quote_count'] = author_counts[item.get('author', 'Unknown')]
            
            return filtered_data[:limit]
                
        except Exception as e:
            return {"error": f"HDFS query error: {e}"}

router = SmartDataRouter()

@app.route('/smart-search/<collection>')
def smart_search(collection):
    """Enhanced intelligent search with multiple field support"""
    query_params = dict(request.args)
    query = query_params.get('q', '')
    limit = int(query_params.get('limit', 50))
    
    # Advanced filters
    keyword_filter = query_params.get('keyword', '')
    country_filter = query_params.get('country', '')
    city_filter = query_params.get('city', '')
    year_filter = query_params.get('year', '')
    affiliation_filter = query_params.get('affiliation', '')
    
    # Decide which technology to use
    use_hdfs, reason = router.should_use_hdfs(collection, query_params)
    
    start_time = time.time()
    
    if use_hdfs:
        # Use HDFS + Spark for complex queries
        results = router.query_hdfs_with_spark(collection, query_params)
        source = "HDFS + Spark"
    else:
        # Use MongoDB for simple queries with comprehensive search
        try:
            # Build dynamic search query
            search_conditions = []
            
            # Text search across multiple fields (case-insensitive)
            if query:
                search_conditions.append({
                    "$or": [
                        {"keyword": {"$regex": query, "$options": "i"}},
                        {"title": {"$regex": query, "$options": "i"}},
                        {"authors": {"$regex": query, "$options": "i"}},
                        {"abstract": {"$regex": query, "$options": "i"}},
                        {"country": {"$regex": query, "$options": "i"}},
                        {"city": {"$regex": query, "$options": "i"}},
                        {"affiliation": {"$regex": query, "$options": "i"}},
                        {"year": {"$regex": query, "$options": "i"}}
                    ]
                })
            
            # Specific field filters (case-insensitive)
            if keyword_filter:
                search_conditions.append({"keyword": {"$regex": keyword_filter, "$options": "i"}})
            if country_filter:
                search_conditions.append({"country": {"$regex": country_filter, "$options": "i"}})
            if city_filter:
                search_conditions.append({"city": {"$regex": city_filter, "$options": "i"}})
            if year_filter:
                search_conditions.append({"year": {"$regex": year_filter, "$options": "i"}})
            if affiliation_filter:
                search_conditions.append({"affiliation": {"$regex": affiliation_filter, "$options": "i"}})
            
            # Combine all conditions
            if search_conditions:
                if len(search_conditions) == 1:
                    search_query = search_conditions[0]
                else:
                    search_query = {"$and": search_conditions}
            else:
                search_query = {}
            
            results = list(db[collection].find(search_query).limit(limit))
            
            # Convert ObjectId to string
            for item in results:
                if '_id' in item:
                    item['_id'] = str(item['_id'])
                    
        except Exception as e:
            print(f"ERROR in MongoDB search: {e}")
            results = []
        
        source = "MongoDB"
    
    execution_time = time.time() - start_time
    
    return jsonify({
        "query": query_params,
        "collection": collection,
        "source": source,
        "reason": reason,
        "execution_time": f"{execution_time:.2f}s",
        "results": results,
        "count": len(results) if isinstance(results, list) else 0
    })

@app.route('/')
def home():
    return jsonify({
        "message": "Smart Data Router - Enhanced API",
        "version": "2.0",
        "features": ["intelligent_routing", "mongodb", "hdfs", "analytics", "real_time"],
        "endpoints": [
            "/smart-search/<collection>",
            "/data-stats",
            "/collections",
            "/data/<collection>",
            "/stats/<collection>",
            "/analytics",
            "/keywords",
            "/countries",
            "/years",
            "/recent"
        ]
    })

@app.route('/collections')
def get_collections():
    """Get all collections with detailed stats"""
    collections = db.list_collection_names()
    stats = {}
    for col in collections:
        count = db[col].count_documents({})
        sample = db[col].find_one()
        data_type = "unknown"
        if sample:
            if 'keyword' in sample and 'authors' in sample:
                data_type = "research_papers"
            elif 'text' in sample and 'author' in sample:
                data_type = "quotes"
        
        stats[col] = {
            "count": count,
            "data_type": data_type,
            "recommended_source": "HDFS" if count > router.mongo_threshold else "MongoDB"
        }
    
    return jsonify({"collections": collections, "stats": stats})

@app.route('/data/<collection>')
def get_data(collection):
    """Get paginated data from collection"""
    try:
        limit = int(request.args.get('limit', 100))
        skip = int(request.args.get('skip', 0))
        
        data = list(db[collection].find().skip(skip).limit(limit))
        
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
    """Get detailed statistics for a collection"""
    try:
        total = db[collection].count_documents({})
        recent = db[collection].count_documents({
            "stored_at": {"$gte": datetime.now().replace(hour=0, minute=0, second=0)}
        })
        
        sample = db[collection].find_one()
        dynamic_stats = {}
        
        if sample:
            if 'keyword' in sample:
                keywords = db[collection].distinct('keyword')
                countries = db[collection].distinct('country')
                years = db[collection].distinct('year')
                affiliations = db[collection].distinct('affiliation')
                dynamic_stats = {
                    "unique_keywords": len(keywords),
                    "unique_countries": len(countries),
                    "unique_affiliations": len(affiliations),
                    "year_range": f"{min(years)} - {max(years)}" if years else "N/A",
                    "keywords": keywords,
                    "countries": countries
                }
            elif 'author' in sample:
                authors = db[collection].distinct('author')
                dynamic_stats = {"unique_authors": len(authors)}
        
        return jsonify({
            "collection": collection,
            "total_documents": total,
            "recent_documents": recent,
            "dynamic_stats": dynamic_stats,
            "last_updated": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/keywords')
def get_keywords():
    """Get keyword distribution"""
    try:
        pipeline = [
            {"$group": {"_id": "$keyword", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        results = list(db.raw_data.aggregate(pipeline))
        return jsonify({"keywords": [{"keyword": r["_id"], "count": r["count"]} for r in results]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/countries')
def get_countries():
    """Get country distribution"""
    try:
        pipeline = [
            {"$group": {"_id": "$country", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        results = list(db.raw_data.aggregate(pipeline))
        return jsonify({"countries": [{"country": r["_id"], "count": r["count"]} for r in results]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/years')
def get_years():
    """Get year distribution"""
    try:
        pipeline = [
            {"$group": {"_id": "$year", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        results = list(db.raw_data.aggregate(pipeline))
        return jsonify({"years": [{"year": r["_id"], "count": r["count"]} for r in results]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/recent')
def get_recent():
    """Get recently added papers"""
    try:
        limit = int(request.args.get('limit', 10))
        recent_docs = list(db.raw_data.find().sort("stored_at", -1).limit(limit))
        
        for item in recent_docs:
            if '_id' in item:
                item['_id'] = str(item['_id'])
        
        return jsonify({"recent": recent_docs, "count": len(recent_docs)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/analytics')
def get_analytics():
    """Get comprehensive analytics"""
    try:
        analysis_file = "/app/data/analysis_results.json"
        if os.path.exists(analysis_file):
            with open(analysis_file, 'r') as f:
                analytics_data = json.load(f)
            return jsonify(analytics_data)
        else:
            return jsonify({
                "message": "No analytics data available",
                "status": "no_data"
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/data-stats')
def data_stats():
    """Statistiques pour aider le routage"""
    collections = db.list_collection_names()
    stats = {}
    
    for col in collections:
        count = db[col].count_documents({})
        stats[col] = {
            "documents": count,
            "recommended_source": "HDFS" if count > router.mongo_threshold else "MongoDB",
            "size_category": "Large" if count > router.mongo_threshold else "Small"
        }
    
    return jsonify({
        "collections": stats,
        "thresholds": {
            "mongo_threshold": router.mongo_threshold,
            "complex_query_threshold": router.complex_query_threshold
        }
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)