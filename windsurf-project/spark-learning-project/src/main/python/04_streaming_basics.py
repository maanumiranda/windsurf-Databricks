#!/usr/bin/env python3
"""
Apache Spark Structured Streaming Basics
This script demonstrates fundamental streaming operations in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, window, count, sum as spark_sum, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import sys
import time
import threading
import json
import random

def create_spark_session():
    """Create and return a Spark session optimized for streaming."""
    return SparkSession.builder \
        .appName("Streaming Basics") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .getOrCreate()

def create_sample_data_stream():
    """Create a thread that generates sample streaming data."""
    def generate_data():
        """Generate sample data and write to files for streaming."""
        import os
        
        # Create directory for streaming data
        stream_dir = "/tmp/streaming_data"
        os.makedirs(stream_dir, exist_ok=True)
        
        products = ["laptop", "phone", "tablet", "watch", "headphones", "speaker", "camera", "router"]
        customers = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
        
        for i in range(50):  # Generate 50 batches
            timestamp = int(time.time() * 1000)
            batch_data = []
            
            # Generate 5-15 random records per batch
            for _ in range(random.randint(5, 15)):
                record = {
                    "timestamp": timestamp,
                    "customer": random.choice(customers),
                    "product": random.choice(products),
                    "quantity": random.randint(1, 5),
                    "price": round(random.uniform(10.0, 1000.0), 2),
                    "category": random.choice(["electronics", "accessories", "gadgets"])
                }
                batch_data.append(record)
            
            # Write batch to file
            filename = f"{stream_dir}/batch_{i}_{timestamp}.json"
            with open(filename, "w") as f:
                for record in batch_data:
                    f.write(json.dumps(record) + "\n")
            
            print(f"Generated batch {i} with {len(batch_data)} records")
            time.sleep(2)  # Wait 2 seconds between batches
    
    # Start data generation in a separate thread
    data_thread = threading.Thread(target=generate_data)
    data_thread.daemon = True
    data_thread.start()
    return data_thread

def basic_streaming_example(spark):
    """Demonstrate basic structured streaming."""
    print("=== Basic Streaming Example ===")
    
    # Define schema for the streaming data
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("customer", StringType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("category", StringType(), True)
    ])
    
    # Read streaming data from files
    streaming_df = spark.readStream \
        .schema(schema) \
        .option("maxFilesPerTrigger", 1) \
        .json("/tmp/streaming_data")
    
    # Add processing timestamp
    processed_df = streaming_df.withColumn("processing_time", current_timestamp())
    
    # Start the streaming query
    query = processed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    # Let it run for 30 seconds
    time.sleep(30)
    query.stop()
    print("Basic streaming example completed")

def windowed_aggregations(spark):
    """Demonstrate windowed aggregations on streaming data."""
    print("\n=== Windowed Aggregations Example ===")
    
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("customer", StringType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("category", StringType(), True)
    ])
    
    # Read streaming data
    streaming_df = spark.readStream \
        .schema(schema) \
        .option("maxFilesPerTrigger", 1) \
        .json("/tmp/streaming_data")
    
    # Calculate total amount
    streaming_df = streaming_df.withColumn("total_amount", col("quantity") * col("price"))
    
    # Windowed aggregation by category
    windowed_counts = streaming_df.groupBy(
        window(col("timestamp"), "10 seconds", "5 seconds"),
        col("category")
    ).agg(
        count("*").alias("transaction_count"),
        spark_sum("total_amount").alias("total_sales"),
        avg("price").alias("avg_price")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("transaction_count"),
        col("total_sales"),
        col("avg_price")
    )
    
    # Start the streaming query
    query = windowed_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    # Let it run for 35 seconds
    time.sleep(35)
    query.stop()
    print("Windowed aggregations example completed")

def watermarks_example(spark):
    """Demonstrate watermarks for handling late data."""
    print("\n=== Watermarks Example ===")
    
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("customer", StringType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("category", StringType(), True)
    ])
    
    # Read streaming data with watermark
    streaming_df = spark.readStream \
        .schema(schema) \
        .option("maxFilesPerTrigger", 1) \
        .json("/tmp/streaming_data")
    
    # Apply watermark and windowed aggregation
    watermarked_df = streaming_df.withWatermark("timestamp", "30 seconds")
    
    # Windowed aggregation with watermark
    windowed_sales = watermarked_df.groupBy(
        window(col("timestamp"), "15 seconds"),
        col("product")
    ).agg(
        count("*").alias("sales_count"),
        spark_sum(col("quantity") * col("price")).alias("total_revenue")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("product"),
        col("sales_count"),
        col("total_revenue")
    ).orderBy(col("window_start"), col("total_revenue").desc())
    
    # Start the streaming query
    query = windowed_sales.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    # Let it run for 30 seconds
    time.sleep(30)
    query.stop()
    print("Watermarks example completed")

def foreach_batch_example(spark):
    """Demonstrate foreachBatch for custom processing."""
    print("\n=== ForeachBatch Example ===")
    
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("customer", StringType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("category", StringType(), True)
    ])
    
    def process_batch(batch_df, batch_id):
        """Process each micro-batch."""
        if batch_df.count() > 0:
            print(f"Processing batch {batch_id} with {batch_df.count()} records")
            
            # Show sample records
            batch_df.show(3, truncate=False)
            
            # Calculate batch statistics
            batch_stats = batch_df.agg(
                count("*").alias("record_count"),
                avg("price").alias("avg_price"),
                spark_sum(col("quantity") * col("price")).alias("total_revenue")
            ).collect()[0]
            
            print(f"Batch {batch_id} stats:")
            print(f"  Records: {batch_stats.record_count}")
            print(f"  Average price: {batch_stats.avg_price:.2f}")
            print(f"  Total revenue: {batch_stats.total_revenue:.2f}")
            print("-" * 50)
    
    # Read streaming data
    streaming_df = spark.readStream \
        .schema(schema) \
        .option("maxFilesPerTrigger", 1) \
        .json("/tmp/streaming_data")
    
    # Start streaming with foreachBatch
    query = streaming_df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime="5 seconds") \
        .start()
    
    # Let it run for 25 seconds
    time.sleep(25)
    query.stop()
    print("ForeachBatch example completed")

def write_to_memory_example(spark):
    """Demonstrate writing stream to memory for querying."""
    print("\n=== Write to Memory Example ===")
    
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("customer", StringType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("category", StringType(), True)
    ])
    
    # Read streaming data
    streaming_df = spark.readStream \
        .schema(schema) \
        .option("maxFilesPerTrigger", 1) \
        .json("/tmp/streaming_data")
    
    # Add calculated fields
    enriched_df = streaming_df.withColumn("total_amount", col("quantity") * col("price"))
    
    # Write to memory sink
    query = enriched_df.writeStream \
        .outputMode("complete") \
        .format("memory") \
        .queryName("sales_stream") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    # Query the memory table periodically
    for i in range(6):  # Query 6 times over 30 seconds
        time.sleep(5)
        print(f"\nQuery {i+1} - Sales by category:")
        spark.sql("""
            SELECT 
                category,
                COUNT(*) as transaction_count,
                SUM(total_amount) as total_sales,
                AVG(total_amount) as avg_transaction
            FROM sales_stream
            GROUP BY category
            ORDER BY total_sales DESC
        """).show()
    
    query.stop()
    print("Memory sink example completed")

def main():
    """Main function to run streaming examples."""
    spark = create_spark_session()
    
    try:
        print("Starting Spark Streaming Examples...")
        print("Note: These examples will generate sample data and process it in real-time")
        print("Each example will run for approximately 30 seconds\n")
        
        # Start data generation
        data_thread = create_sample_data_stream()
        
        # Wait a bit for initial data
        time.sleep(3)
        
        # Run streaming examples
        basic_streaming_example(spark)
        windowed_aggregations(spark)
        watermarks_example(spark)
        foreach_batch_example(spark)
        write_to_memory_example(spark)
        
        print("\nAll streaming examples completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
