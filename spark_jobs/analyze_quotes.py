import json
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, length, explode
from pyspark.sql.types import StringType, StructType, StructField

def run_dynamic_analysis():
    spark = SparkSession.builder \
        .appName("DynamicDataAnalysis") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    try:
        # Read all JSON files dynamically
        import glob
        json_files = glob.glob("/app/data/research*.json")
        
        if not json_files:
            print("No research data files found!")
            # Fallback to quotes files for backward compatibility
            json_files = glob.glob("/app/data/quotes*.json")
            
        if not json_files:
            print("No JSON files found!")
            return
        
        print(f"Found {len(json_files)} data files to process")
        
        # Read and union all files
        df = spark.read.json(json_files[0])
        for json_file in json_files[1:]:
            df_temp = spark.read.json(json_file)
            df = df.union(df_temp)
        
        # Remove duplicates based on title or text
        if 'title' in df.columns:
            df = df.dropDuplicates(["title"])
        elif 'text' in df.columns:
            df = df.dropDuplicates(["text", "author"])
        
        print(f"=== DYNAMIC DATA ANALYSIS ===")
        print(f"Total records: {df.count()}")
        
        # Show available columns
        print(f"Available columns: {df.columns}")
        
        # Dynamic analysis based on available columns
        columns = df.columns
        
        if 'keyword' in columns:
            print("\n=== KEYWORD ANALYSIS ===")
            keyword_stats = df.groupBy("keyword").count().orderBy(desc("count"))
            keyword_stats.show(10)
        
        if 'year' in columns:
            print("\n=== YEAR TRENDS ===")
            year_stats = df.groupBy("year").count().orderBy("year")
            year_stats.show(10)
        
        if 'country' in columns:
            print("\n=== COUNTRY DISTRIBUTION ===")
            country_stats = df.groupBy("country").count().orderBy(desc("count"))
            country_stats.show(10)
        
        if 'authors' in columns:
            print("\n=== TOP AUTHORS ===")
            author_stats = df.groupBy("authors").count().orderBy(desc("count"))
            author_stats.show(10)
        elif 'author' in columns:
            print("\n=== AUTHOR ANALYSIS ===")
            author_stats = df.groupBy("author").count().orderBy(desc("count"))
            author_stats.show(10)
        
        if 'affiliation' in columns:
            print("\n=== UNIVERSITY AFFILIATIONS ===")
            aff_stats = df.groupBy("affiliation").count().orderBy(desc("count"))
            aff_stats.show(10)
        
        if 'tags' in columns:
            print("\n=== TAG ANALYSIS ===")
            df.select(explode(col("tags")).alias("tag")) \
              .groupBy("tag").count() \
              .orderBy(desc("count")) \
              .show(15)
        
        # Save results for API consumption
        results_data = {
            "total_records": df.count(),
            "data_type": "research_papers" if 'keyword' in columns else "quotes",
            "columns": columns
        }
        
        # Export aggregated data
        if 'keyword' in columns:
            keyword_data = df.groupBy("keyword").count().collect()
            results_data["keyword_stats"] = [row.asDict() for row in keyword_data]
        
        if 'year' in columns:
            year_data = df.groupBy("year").count().collect()
            results_data["year_stats"] = [row.asDict() for row in year_data]
        
        if 'country' in columns:
            country_data = df.groupBy("country").count().collect()
            results_data["country_stats"] = [row.asDict() for row in country_data]
        
        # Save to JSON for API
        with open("/app/data/analysis_results.json", "w") as f:
            json.dump(results_data, f, indent=2)
        
        # Save detailed results to HDFS-style directory
        df.coalesce(1).write.mode("overwrite").json("/app/data/spark_results/analysis_results")
        
        print("\n=== ANALYSIS COMPLETE ===")
        print("Results saved to analysis_results.json")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    run_dynamic_analysis()