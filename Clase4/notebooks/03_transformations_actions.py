"""
Spark Transformations vs Actions: Understanding Lazy Evaluation
===============================================================

This notebook demonstrates the fundamental concepts of Spark's execution model:
- Transformations: Lazy operations that build up the execution plan
- Actions: Eager operations that trigger computation
- Understanding when and how Spark executes operations
- Performance implications and optimization strategies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# Initialize Spark Session with configuration for monitoring
spark = SparkSession.builder \
    .appName("Transformations vs Actions") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Set log level to see execution details
spark.sparkContext.setLogLevel("INFO")

print("✅ Spark Session Created Successfully!")
print("🔍 Monitoring execution to understand lazy evaluation...")

# Load data
netflix_df = spark.read.csv(
    "../data/netflix_data.csv",
    header=True,
    inferSchema=True,
    quote='"',
    escape='"'
)

print(f"📊 Base dataset: {netflix_df.count()} rows")

# ============================================================================
# UNDERSTANDING TRANSFORMATIONS
# ============================================================================

print("\n" + "="*80)
print("🔄 TRANSFORMATIONS (Lazy Operations)")
print("="*80)

print("""
📚 What are Transformations?
• Transformations are LAZY operations that build up a logical plan
• They DON'T execute immediately when called
• They return a new DataFrame/ RDD with the transformation applied
• Spark builds up a DAG (Directed Acyclic Graph) of transformations
• Execution only happens when an ACTION is called

🎯 Common Transformations:
• select(), filter(), withColumn(), drop()
• groupBy(), orderBy(), join(), union()
• distinct(), dropDuplicates()
• map(), flatMap() (for RDDs)
""")

# Demonstrate transformation chaining (no execution yet)
print("\n🔗 Building transformation pipeline...")

# Step 1: Filter for recent content
step1_df = netflix_df.filter(col("release_year") >= 2015)
print("1️⃣ Filtered for content from 2015+ (Transformation only)")

# Step 2: Select specific columns
step2_df = step1_df.select(
    "title", "type", "release_year", "rating", "country", "listed_in"
)
print("2️⃣ Selected specific columns (Transformation only)")

# Step 3: Add computed columns
step3_df = step2_df.withColumn(
    "title_length", length(col("title"))
).withColumn(
    "genre_count", size(split(col("listed_in"), ","))
)
print("3️⃣ Added computed columns (Transformation only)")

# Step 4: Filter by title length
step4_df = step3_df.filter(col("title_length") > 10)
print("4️⃣ Filtered by title length (Transformation only)")

# Step 5: Order by multiple columns
final_transformed_df = step4_df.orderBy(
    col("release_year").desc(),
    col("title_length").desc()
)
print("5️⃣ Ordered by year and title length (Transformation only)")

print(f"\n📋 Transformation pipeline built!")
print(f"   • No computation has happened yet")
print(f"   • Spark has created an execution plan")
print(f"   • Ready for action execution")

# Show the execution plan (no execution)
print("\n📊 Execution Plan (Lazy Evaluation):")
final_transformed_df.explain(extended=False)

# ============================================================================
# UNDERSTANDING ACTIONS
# ============================================================================

print("\n" + "="*80)
print("⚡ ACTIONS (Eager Operations)")
print("="*80)

print("""
📚 What are Actions?
• Actions are EAGER operations that trigger computation
• They FORCE Spark to execute the transformation pipeline
• They return values to the driver program
• They cause the DAG to be executed on the cluster

🎯 Common Actions:
• show(), collect(), take(), first(), head()
• count(), reduce(), foreach()
• write() (save operations)
• saveAsTextFile(), saveAsTable() (for RDDs)
""")

# Action 1: show() - triggers execution
print("\n1️⃣ Action: show() - Displaying results")
print("⏰ Executing transformation pipeline...")

start_time = time.time()
final_transformed_df.show(10, truncate=False)
end_time = time.time()

print(f"⚡ Execution time for show(): {end_time - start_time:.2f} seconds")
print("✅ Transformation pipeline executed!")

# Action 2: count() - triggers execution
print("\n2️⃣ Action: count() - Counting results")

# Build a new transformation pipeline
count_pipeline = netflix_df.filter(
    col("type") == "Movie"
).filter(
    col("release_year") >= 2020
).filter(
    col("rating").isin("PG-13", "R")
)

print("🔗 Built pipeline for counting recent movies (Transformations only)")

start_time = time.time()
movie_count = count_pipeline.count()
end_time = time.time()

print(f"📊 Recent movies (2020+, PG-13/R): {movie_count}")
print(f"⚡ Execution time for count(): {end_time - start_time:.2f} seconds")

# Action 3: collect() - triggers execution and brings data to driver
print("\n3️⃣ Action: collect() - Bringing data to driver")

# Build another pipeline
collect_pipeline = netflix_df.filter(
    col("country") == "United States"
).filter(
    col("type") == "TV Show"
).select("title", "release_year", "rating").orderBy(col("release_year").desc())

print("🔗 Built pipeline for US TV shows (Transformations only)")

start_time = time.time()
us_tv_shows = collect_pipeline.collect()  # This brings data to driver
end_time = time.time()

print(f"📺 US TV Shows collected: {len(us_tv_shows)} titles")
print(f"⚡ Execution time for collect(): {end_time - start_time:.2f} seconds")
print("⚠️  Warning: collect() brings all data to driver - use with large datasets!")

# ============================================================================
# TRANSFORMATION PIPELINE REUSE
# ============================================================================

print("\n" + "="*80)
print("🔄 TRANSFORMATION PIPELINE REUSE")
print("="*80)

print("""
📚 Pipeline Reuse Benefits:
• Transformations can be reused multiple times
• Each action re-executes the entire pipeline
• Caching can optimize repeated executions
• Understanding this helps with performance tuning
""")

# Create a reusable transformation pipeline
print("🔗 Creating reusable transformation pipeline...")

base_pipeline = netflix_df.filter(
    col("release_year") >= 2010
).withColumn(
    "content_age", year(current_date()) - col("release_year")
).withColumn(
    "title_length", length(col("title"))
).withColumn(
    "has_long_title", col("title_length") > 20
)

print("✅ Base pipeline created (no execution yet)")

# Multiple actions on the same pipeline
print("\n📊 Executing multiple actions on same pipeline:")

# Action 1: Count
start_time = time.time()
total_count = base_pipeline.count()
end_time = time.time()
print(f"   • Count: {total_count} records (Time: {end_time - start_time:.2f}s)")

# Action 2: Show sample
start_time = time.time()
base_pipeline.select("title", "release_year", "content_age", "title_length").show(5, truncate=False)
end_time = time.time()
print(f"   • Show sample (Time: {end_time - start_time:.2f}s)")

# Action 3: Aggregate
start_time = time.time()
stats = base_pipeline.groupBy("type").agg(
    count("*").alias("count"),
    avg("content_age").alias("avg_age"),
    avg("title_length").alias("avg_title_length")
).collect()
end_time = time.time()
print(f"   • Aggregate stats (Time: {end_time - start_time:.2f}s")

for stat in stats:
    print(f"     {stat.type}: {stat.count} titles, avg age: {stat.avg_age:.1f}, avg title length: {stat.avg_title_length:.1f}")

print("\n💡 Observation: Each action re-executed the entire pipeline!")

# ============================================================================
# CACHING FOR PERFORMANCE
# ============================================================================

print("\n" + "="*80)
print("💾 CACHING FOR PERFORMANCE OPTIMIZATION")
print("="*80)

print("""
📚 Caching Benefits:
• Persists intermediate results in memory/disk
• Avoids re-computation of expensive transformations
• Significantly speeds up repeated actions
• Trade-off: Memory usage vs. computation time

🎯 Caching Strategies:
• cache() - default storage level (MEMORY_AND_DISK)
• persist(StorageLevel) - specific storage levels
• unpersist() - free up memory when done
""")

# Create and cache a pipeline
print("🔗 Creating and caching pipeline...")

cached_pipeline = netflix_df.filter(
    col("release_year") >= 2015
).withColumn(
    "decade", (col("release_year") / 10).cast(IntegerType()) * 10
).withColumn(
    "is_recent", col("release_year") >= 2020
).filter(
    col("country").isNotNull()
).cache()  # Cache the result

print("✅ Pipeline cached (first execution will populate cache)")

# First action - populates cache
print("\n📊 First action (populates cache):")
start_time = time.time()
first_count = cached_pipeline.count()
end_time = time.time()
print(f"   • Count: {first_count} records (Time: {end_time - start_time:.2f}s) - Cache populated")

# Subsequent actions - use cache
print("\n📊 Subsequent actions (use cache):")

start_time = time.time()
cached_pipeline.groupBy("type").count().show()
end_time = time.time()
print(f"   • Group by type (Time: {end_time - start_time:.2f}s) - Using cache")

start_time = time.time()
cached_pipeline.filter(col("is_recent") == True).count()
end_time = time.time()
print(f"   • Count recent content (Time: {end_time - start_time:.2f}s) - Using cache")

start_time = time.time()
cached_pipeline.groupBy("decade").count().orderBy("decade").show()
end_time = time.time()
print(f"   • Group by decade (Time: {end_time - start_time:.2f}s) - Using cache")

# Clean up cache
cached_pipeline.unpersist()
print("\n🧹 Cache cleared")

# ============================================================================
# WIDE vs NARROW TRANSFORMATIONS
# ============================================================================

print("\n" + "="*80)
print("🌐 WIDE vs NARROW TRANSFORMATIONS")
print("="*80)

print("""
📚 Transformation Types:
• Narrow Transformations:
  - Each input partition contributes to at most one output partition
  - No data shuffling required
  - Examples: map(), filter(), flatMap(), select(), withColumn()
  - Fast and efficient

• Wide Transformations:
  - Input partitions can contribute to multiple output partitions
  - Requires data shuffling across the network
  - Examples: groupBy(), reduceByKey(), join(), orderBy(), distinct()
  - More expensive due to network I/O
""")

# Narrow transformation example
print("\n🔹 Narrow Transformation Example:")
print("   • Operation: filter() + select() + withColumn()")

narrow_df = netflix_df.filter(col("type") == "Movie") \
    .select("title", "release_year", "rating") \
    .withColumn("title_upper", upper(col("title")))

print("   • No shuffling required - each partition processed independently")
narrow_df.show(5, truncate=False)

# Wide transformation example
print("\n🔸 Wide Transformation Example:")
print("   • Operation: groupBy() + count()")

print("   • Requires shuffling - data with same keys must be brought together")
wide_result = netflix_df.groupBy("type").count()
wide_result.show(truncate=False)

# Another wide transformation
print("\n🔸 Wide Transformation Example:")
print("   • Operation: orderBy()")

print("   • Requires shuffling - global ordering needs all data")
ordered_result = netflix_df.orderBy(col("release_year").desc())
ordered_result.select("title", "release_year").show(5, truncate=False)

# ============================================================================
# PERFORMANCE MONITORING
# ============================================================================

print("\n" + "="*80)
print("📊 PERFORMANCE MONITORING")
print("="*80)

print("🔍 Monitoring transformation performance...")

# Create a complex pipeline with both narrow and wide transformations
complex_pipeline = netflix_df.filter(col("release_year") >= 2000) \
    .withColumn("title_length", length(col("title"))) \
    .withColumn("genre_count", size(split(col("listed_in"), ","))) \
    .filter(col("title_length") > 5) \
    .groupBy("type", "rating") \
    .agg(
        count("*").alias("total_titles"),
        avg("title_length").alias("avg_title_length"),
        avg("genre_count").alias("avg_genre_count")
    ) \
    .orderBy(col("total_titles").desc())

# Show execution plan
print("\n📋 Detailed Execution Plan:")
complex_pipeline.explain(extended=True)

# Execute and time
print("\n⏰ Executing complex pipeline...")
start_time = time.time()
results = complex_pipeline.collect()
end_time = time.time()

print(f"✅ Execution completed in {end_time - start_time:.2f} seconds")
print("\n📊 Results:")
for result in results:
    print(f"   {result.type} - {result.rating}: {result.total_titles} titles")

# ============================================================================
# BEST PRACTICES
# ============================================================================

print("\n" + "="*80)
print("💡 BEST PRACTICES")
print("="*80)

print("""
🎯 Transformation Best Practices:
1. Filter early - reduce data size as soon as possible
2. Select specific columns - avoid carrying unnecessary data
3. Use appropriate join types - broadcast joins for small tables
4. Repartition strategically - avoid data skew
5. Cache intermediate results when reused multiple times

🎯 Action Best Practices:
1. Use take() or limit() instead of collect() for large datasets
2. Avoid collect() on very large DataFrames
3. Use write() for persistent storage instead of collect()
4. Monitor driver memory when using collect()
5. Use foreach() for side effects without collecting data

🎯 Performance Tips:
1. Understand narrow vs wide transformations
2. Minimize shuffles in your pipelines
3. Use appropriate caching strategies
4. Monitor Spark UI for execution details
5. Test with sample data before full-scale execution
""")

# Demonstrate best practice: Filter early
print("\n💡 Best Practice Example: Filter Early")

# Bad: Process all data then filter
print("❌ Bad approach - Process all data first:")
start_time = time.time()
bad_result = netflix_df.withColumn("title_length", length(col("title"))) \
    .withColumn("genre_count", size(split(col("listed_in"), ","))) \
    .filter(col("release_year") >= 2020) \
    .count()
end_time = time.time()
print(f"   • Time: {end_time - start_time:.2f}s (processed all data)")

# Good: Filter first then process
print("✅ Good approach - Filter first:")
start_time = time.time()
good_result = netflix_df.filter(col("release_year") >= 2020) \
    .withColumn("title_length", length(col("title"))) \
    .withColumn("genre_count", size(split(col("listed_in"), ","))) \
    .count()
end_time = time.time()
print(f"   • Time: {end_time - start_time:.2f}s (filtered first)")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*80)
print("📋 TRANSFORMATIONS vs ACTIONS SUMMARY")
print("="*80)

print("""
🔄 TRANSFORMATIONS (Lazy):
• Build execution plans without executing
• Return new DataFrames/RDDs
• Examples: select, filter, withColumn, groupBy, orderBy
• No computation until action is called
• Can be chained together efficiently

⚡ ACTIONS (Eager):
• Trigger execution of transformation pipeline
• Return results to driver program
• Examples: show, count, collect, take, write
• Cause actual computation to happen
• Results can be expensive for large datasets

🎯 KEY CONCEPTS:
• Lazy Evaluation: Spark optimizes before execution
• DAG Execution: Transformations build a Directed Acyclic Graph
• Pipeline Reuse: Same transformations can be used for multiple actions
• Caching: Store intermediate results for performance
• Wide vs Narrow: Understand data shuffling implications

💡 PERFORMANCE INSIGHTS:
• Filter and select early to reduce data size
• Cache frequently used intermediate results
• Use take() instead of collect() for large datasets
• Monitor Spark UI for execution details
• Understand when shuffling occurs
""")

# Final demonstration
print("\n🎯 Final Demonstration - Complete Pipeline:")

# Build complex transformation pipeline
final_pipeline = netflix_df.filter(col("release_year") >= 2010) \
    .filter(col("country").isNotNull()) \
    .withColumn("title_length", length(col("title"))) \
    .withColumn("decade", (col("release_year") / 10).cast(IntegerType()) * 10) \
    .withColumn("is_long_title", col("title_length") > 20) \
    .filter(col("title_length") > 5)

print("🔗 Transformation pipeline built (no execution yet)")

# Multiple actions on the same pipeline
print("\n📊 Executing actions:")

# Action 1: Basic statistics
stats = final_pipeline.select(
    count("*").alias("total_records"),
    avg("title_length").alias("avg_title_length"),
    max("title_length").alias("max_title_length"),
    count(when(col("is_long_title") == True, True)).alias("long_titles")
).first()

print(f"   • Total records: {stats.total_records}")
print(f"   • Average title length: {stats.avg_title_length:.1f}")
print(f"   • Maximum title length: {stats.max_title_length}")
print(f"   • Long titles (>20 chars): {stats.long_titles}")

# Action 2: Group by decade
decade_stats = final_pipeline.groupBy("decade").agg(
    count("*").alias("title_count"),
    avg("title_length").alias("avg_length")
).orderBy("decade")

print("\n   • Content by decade:")
decade_stats.show(truncate=False)

print("\n✅ Transformations vs Actions demonstration complete!")
print("🎓 You now understand Spark's lazy evaluation model!")
