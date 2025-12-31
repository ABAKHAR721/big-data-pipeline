from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, count, desc, when, length
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("QuotesAnalysis") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

try:
    # Read JSON data - read all files individually and union them
    import glob
    json_files = glob.glob("/app/data/quotes*.json")
    
    if not json_files:
        print("No JSON files found!")
        spark.stop()
        exit(1)
    
    # Read first file
    df = spark.read.json(json_files[0])
    
    # Union with other files if they exist
    for json_file in json_files[1:]:
        df_temp = spark.read.json(json_file)
        df = df.union(df_temp)
    
    # Remove duplicates
    df = df.dropDuplicates(["text", "author"])
    
    print("=== QUOTES DATA ANALYSIS ===")
    print(f"Total quotes: {df.count()}")
    
    # Advanced Analytics
    print("\n=== QUOTE LENGTH ANALYSIS ===")
    df_with_length = df.withColumn("text_length", length(col("text")))
    df_with_length.select("author", "text_length").groupBy("author") \
        .avg("text_length").orderBy(desc("avg(text_length)")).show(10)
    
    # Author productivity
    print("\n=== AUTHOR PRODUCTIVITY ===")
    author_stats = df.groupBy("author").count().orderBy(desc("count"))
    author_stats.show(10)
    
    # Tag analysis
    print("\n=== TAG TRENDS ===")
    df.select(explode(col("tags")).alias("tag")) \
      .groupBy("tag").count() \
      .orderBy(desc("count")) \
      .show(15)
    
    # Save results to HDFS (when available)
    author_stats.coalesce(1).write.mode("overwrite").json("/app/data/spark_results/author_stats")
    
    print("\n=== ANALYSIS COMPLETE ===")
    
except Exception as e:
    print(f"Error: {e}")

finally:
    spark.stop()