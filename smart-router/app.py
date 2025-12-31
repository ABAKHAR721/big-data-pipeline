from flask import Flask, jsonify, request
import json
import os
from pymongo import MongoClient
import subprocess
import time
from datetime import datetime

app = Flask(__name__)

# MongoDB connection
client = MongoClient('mongodb://mongodb:27017/')
db = client.research_db

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
        """Exécute une requête sur HDFS via lecture directe des fichiers"""
        try:
            query = query_params.get('q', '')
            limit = int(query_params.get('limit', 100))
            
            # Lire les fichiers JSON depuis le volume partagé
            all_data = []
            
            # Lire tous les fichiers JSON du dossier data
            import glob
            json_files = glob.glob('/app/data/quotes*.json')
            
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
            
            # Filtrer selon la requête
            if query:
                filtered_data = [
                    item for item in all_data 
                    if query.lower() in str(item.get('text', '')).lower() 
                    or query.lower() in str(item.get('author', '')).lower()
                ]
            else:
                filtered_data = all_data
            
            # Simuler un traitement Spark (tri, agrégation, etc.)
            # Ici on peut ajouter des analyses complexes
            if 'analytics' in query_params:
                # Exemple d'analyse : compter par auteur
                from collections import Counter
                authors = [item.get('author', 'Unknown') for item in filtered_data]
                author_counts = Counter(authors)
                
                # Ajouter les statistiques aux résultats
                for item in filtered_data:
                    item['author_quote_count'] = author_counts[item.get('author', 'Unknown')]
            
            return filtered_data[:limit]
                
        except Exception as e:
            return {"error": f"HDFS query error: {e}"}

router = SmartDataRouter()

@app.route('/smart-search/<collection>')
def smart_search(collection):
    """Routage intelligent entre MongoDB et HDFS"""
    query_params = dict(request.args)
    
    # Décider quelle technologie utiliser
    use_hdfs, reason = router.should_use_hdfs(collection, query_params)
    
    start_time = time.time()
    
    if use_hdfs:
        # Utiliser HDFS + Spark pour les requêtes massives
        results = router.query_hdfs_with_spark(collection, query_params)
        source = "HDFS + Spark"
    else:
        # Utiliser MongoDB pour les requêtes simples
        query = query_params.get('q', '')
        limit = int(query_params.get('limit', 50))
        
        if query:
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
        else:
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