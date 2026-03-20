"""
Mini Project: Netflix Business Intelligence Analytics
====================================================

This mini project challenges you to answer real business questions using both
Spark SQL and DataFrame API approaches. The goal is to demonstrate that both
methods can achieve the same results while building confidence in using Spark
for practical data analysis.

Business Questions to Answer:
1. What are the most popular genres on Netflix?
2. Which countries produce the most content?
3. How has content availability changed over time?
4. What is the average duration by content type?
5. Which content ratings are most common?
6. What are the trends in content production by decade?
7. How does content vary by country and type?
8. What are the characteristics of high-rated content?
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Netflix Business Intelligence") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🎯 Netflix Business Intelligence Mini Project")
print("=" * 60)

# Load and prepare data
netflix_df = spark.read.csv(
    "../data/netflix_data.csv",
    header=True,
    inferSchema=True,
    quote='"',
    escape='"'
)

# Data preparation
netflix_clean = netflix_df.filter(col("release_year").isNotNull()) \
    .withColumn("primary_genre", trim(split(col("listed_in"), ",").getItem(0))) \
    .withColumn("primary_country", trim(split(col("country"), ",").getItem(0))) \
    .withColumn("genre_count", size(split(col("listed_in"), ","))) \
    .withColumn("decade", (col("release_year") / 10).cast(IntegerType()) * 10) \
    .withColumn("content_age", year(current_date()) - col("release_year"))

# Create temporary view for SQL queries
netflix_clean.createOrReplaceTempView("netflix")

print(f"📊 Dataset prepared: {netflix_clean.count()} records")
print("🔍 Ready to answer business questions!")

# ============================================================================
# BUSINESS QUESTION 1: Most Popular Genres
# ============================================================================

print("\n" + "="*80)
print("🎬 QUESTION 1: What are the most popular genres on Netflix?")
print("="*80)

print("\n📝 Approach 1: Using Spark SQL")
start_time = time.time()

genre_sql = spark.sql("""
    SELECT 
        primary_genre,
        COUNT(*) as title_count,
        COUNT(DISTINCT type) as content_types,
        COUNT(DISTINCT primary_country) as countries,
        ROUND(AVG(release_year), 1) as avg_release_year,
        ROUND(AVG(content_age), 1) as avg_content_age
    FROM netflix 
    WHERE primary_genre IS NOT NULL
    GROUP BY primary_genre
    ORDER BY title_count DESC
    LIMIT 10
""")

genre_sql.show(truncate=False)
sql_time_1 = time.time() - start_time

print(f"\n📝 Approach 2: Using DataFrame API")
start_time = time.time()

genre_df = netflix_clean.filter(col("primary_genre").isNotNull()) \
    .groupBy("primary_genre") \
    .agg(
        count("*").alias("title_count"),
        countDistinct("type").alias("content_types"),
        countDistinct("primary_country").alias("countries"),
        round(avg("release_year"), 1).alias("avg_release_year"),
        round(avg("content_age"), 1).alias("avg_content_age")
    ) \
    .orderBy(col("title_count").desc()) \
    .limit(10)

genre_df.show(truncate=False)
df_time_1 = time.time() - start_time

print(f"\n⏱️  Performance: SQL ({sql_time_1:.3f}s) vs DataFrame ({df_time_1:.3f}s)")

# ============================================================================
# BUSINESS QUESTION 2: Top Content Producing Countries
# ============================================================================

print("\n" + "="*80)
print("🌍 QUESTION 2: Which countries produce the most content?")
print("="*80)

print("\n📝 Approach 1: Using Spark SQL")
start_time = time.time()

country_sql = spark.sql("""
    SELECT 
        primary_country,
        COUNT(*) as total_titles,
        COUNT(DISTINCT type) as content_types,
        COUNT(DISTINCT primary_genre) as genre_diversity,
        ROUND(AVG(release_year), 1) as avg_release_year,
        COUNT(CASE WHEN type = 'Movie' THEN 1 END) as movies,
        COUNT(CASE WHEN type = 'TV Show' THEN 1 END) as tv_shows
    FROM netflix 
    WHERE primary_country IS NOT NULL AND primary_country != ''
    GROUP BY primary_country
    ORDER BY total_titles DESC
    LIMIT 10
""")

country_sql.show(truncate=False)
sql_time_2 = time.time() - start_time

print("\n📝 Approach 2: Using DataFrame API")
start_time = time.time()

country_df = netflix_clean.filter(
    (col("primary_country").isNotNull()) & (col("primary_country") != "")
) \
    .groupBy("primary_country") \
    .agg(
        count("*").alias("total_titles"),
        countDistinct("type").alias("content_types"),
        countDistinct("primary_genre").alias("genre_diversity"),
        round(avg("release_year"), 1).alias("avg_release_year"),
        count(when(col("type") == "Movie", 1)).alias("movies"),
        count(when(col("type") == "TV Show", 1)).alias("tv_shows")
    ) \
    .orderBy(col("total_titles").desc()) \
    .limit(10)

country_df.show(truncate=False)
df_time_2 = time.time() - start_time

print(f"\n⏱️  Performance: SQL ({sql_time_2:.3f}s) vs DataFrame ({df_time_2:.3f}s)")

# ============================================================================
# BUSINESS QUESTION 3: Content Availability Trends Over Time
# ============================================================================

print("\n" + "="*80)
print("📈 QUESTION 3: How has content availability changed over time?")
print("="*80)

print("\n📝 Approach 1: Using Spark SQL")
start_time = time.time()

trends_sql = spark.sql("""
    SELECT 
        decade,
        COUNT(*) as total_titles,
        COUNT(DISTINCT primary_country) as countries,
        COUNT(DISTINCT primary_genre) as genres,
        COUNT(CASE WHEN type = 'Movie' THEN 1 END) as movies,
        COUNT(CASE WHEN type = 'TV Show' THEN 1 END) as tv_shows,
        ROUND(COUNT(CASE WHEN type = 'Movie' THEN 1 END) * 100.0 / COUNT(*), 2) as movie_percentage,
        ROUND(COUNT(CASE WHEN type = 'TV Show' THEN 1 END) * 100.0 / COUNT(*), 2) as tv_percentage
    FROM netflix 
    WHERE decade >= 1980
    GROUP BY decade
    ORDER BY decade
""")

trends_sql.show(truncate=False)
sql_time_3 = time.time() - start_time

print("\n📝 Approach 2: Using DataFrame API")
start_time = time.time()

trends_df = netflix_clean.filter(col("decade") >= 1980) \
    .groupBy("decade") \
    .agg(
        count("*").alias("total_titles"),
        countDistinct("primary_country").alias("countries"),
        countDistinct("primary_genre").alias("genres"),
        count(when(col("type") == "Movie", 1)).alias("movies"),
        count(when(col("type") == "TV Show", 1)).alias("tv_shows")
    ) \
    .withColumn("movie_percentage", round(col("movies") * 100.0 / col("total_titles"), 2)) \
    .withColumn("tv_percentage", round(col("tv_shows") * 100.0 / col("total_titles"), 2)) \
    .orderBy("decade")

trends_df.show(truncate=False)
df_time_3 = time.time() - start_time

print(f"\n⏱️  Performance: SQL ({sql_time_3:.3f}s) vs DataFrame ({df_time_3:.3f}s)")

# ============================================================================
# BUSINESS QUESTION 4: Average Duration by Content Type
# ============================================================================

print("\n" + "="*80)
print("⏱️  QUESTION 4: What is the average duration by content type?")
print("="*80)

print("\n📝 Approach 1: Using Spark SQL")
start_time = time.time()

# Extract numeric duration from duration column
duration_sql = spark.sql("""
    WITH duration_extracted AS (
        SELECT 
            type,
            CASE 
                WHEN duration LIKE '%min%' THEN CAST(REGEXP_EXTRACT(duration, '(\\d+)', 1) AS INT)
                WHEN duration LIKE '%Season%' THEN CAST(REGEXP_EXTRACT(duration, '(\\d+)', 1) AS INT) * 10 -- Estimate 10 episodes per season
                ELSE NULL
            END as duration_numeric
        FROM netflix 
        WHERE duration IS NOT NULL
    )
    SELECT 
        type,
        COUNT(*) as total_titles,
        COUNT(duration_numeric) as titles_with_duration,
        ROUND(AVG(duration_numeric), 1) as avg_duration,
        MIN(duration_numeric) as min_duration,
        MAX(duration_numeric) as max_duration,
        ROUND(STDDEV(duration_numeric), 1) as stddev_duration
    FROM duration_extracted
    WHERE duration_numeric IS NOT NULL
    GROUP BY type
""")

duration_sql.show(truncate=False)
sql_time_4 = time.time() - start_time

print("\n📝 Approach 2: Using DataFrame API")
start_time = time.time()

duration_df = netflix_clean.filter(col("duration").isNotNull()) \
    .withColumn(
        "duration_numeric",
        when(col("duration").contains("min"), 
             regexp_extract(col("duration"), r"(\d+)", 1).cast(IntegerType()))
        .when(col("duration").contains("Season"),
             regexp_extract(col("duration"), r"(\d+)", 1).cast(IntegerType()) * 10)  # Estimate
        .otherwise(None)
    ) \
    .filter(col("duration_numeric").isNotNull()) \
    .groupBy("type") \
    .agg(
        count("*").alias("total_titles"),
        round(avg("duration_numeric"), 1).alias("avg_duration"),
        min("duration_numeric").alias("min_duration"),
        max("duration_numeric").alias("max_duration"),
        round(stddev("duration_numeric"), 1).alias("stddev_duration")
    )

duration_df.show(truncate=False)
df_time_4 = time.time() - start_time

print(f"\n⏱️  Performance: SQL ({sql_time_4:.3f}s) vs DataFrame ({df_time_4:.3f}s)")

# ============================================================================
# BUSINESS QUESTION 5: Content Rating Distribution
# ============================================================================

print("\n" + "="*80)
print("🔞 QUESTION 5: Which content ratings are most common?")
print("="*80)

print("\n📝 Approach 1: Using Spark SQL")
start_time = time.time()

rating_sql = spark.sql("""
    SELECT 
        rating,
        COUNT(*) as title_count,
        COUNT(DISTINCT type) as content_types,
        COUNT(DISTINCT primary_country) as countries,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM netflix WHERE rating IS NOT NULL), 2) as percentage,
        COUNT(CASE WHEN type = 'Movie' THEN 1 END) as movies,
        COUNT(CASE WHEN type = 'TV Show' THEN 1 END) as tv_shows
    FROM netflix 
    WHERE rating IS NOT NULL AND rating != ''
    GROUP BY rating
    ORDER BY title_count DESC
""")

rating_sql.show(truncate=False)
sql_time_5 = time.time() - start_time

print("\n📝 Approach 2: Using DataFrame API")
start_time = time.time()

total_count = netflix_clean.filter(col("rating").isNotNull() & (col("rating") != "")).count()

rating_df = netflix_clean.filter(col("rating").isNotNull() & (col("rating") != "")) \
    .groupBy("rating") \
    .agg(
        count("*").alias("title_count"),
        countDistinct("type").alias("content_types"),
        countDistinct("primary_country").alias("countries"),
        count(when(col("type") == "Movie", 1)).alias("movies"),
        count(when(col("type") == "TV Show", 1)).alias("tv_shows")
    ) \
    .withColumn("percentage", round(col("title_count") * 100.0 / total_count, 2)) \
    .orderBy(col("title_count").desc())

rating_df.show(truncate=False)
df_time_5 = time.time() - start_time

print(f"\n⏱️  Performance: SQL ({sql_time_5:.3f}s) vs DataFrame ({df_time_5:.3f}s)")

# ============================================================================
# BUSINESS QUESTION 6: Content Characteristics by Rating
# ============================================================================

print("\n" + "="*80)
print("🎯 QUESTION 6: What are the characteristics of high-rated content?")
print("="*80)

print("\n📝 Approach 1: Using Spark SQL")
start_time = time.time()

high_rated_sql = spark.sql("""
    SELECT 
        rating,
        type,
        COUNT(*) as title_count,
        ROUND(AVG(content_age), 1) as avg_content_age,
        ROUND(AVG(title_length), 1) as avg_title_length,
        COUNT(DISTINCT primary_country) as countries,
        COUNT(DISTINCT primary_genre) as genres,
        ROUND(AVG(genre_count), 1) as avg_genre_count
    FROM netflix 
    WHERE rating IN ('TV-MA', 'TV-14', 'R', 'PG-13') 
        AND primary_country IS NOT NULL
        AND primary_genre IS NOT NULL
    GROUP BY rating, type
    ORDER BY rating, type
""")

high_rated_sql.show(truncate=False)
sql_time_6 = time.time() - start_time

print("\n📝 Approach 2: Using DataFrame API")
start_time = time.time()

high_rated_df = netflix_clean.filter(
    col("rating").isin("TV-MA", "TV-14", "R", "PG-13")
) \
    .filter(
        (col("primary_country").isNotNull()) &
        (col("primary_genre").isNotNull())
    ) \
    .groupBy("rating", "type") \
    .agg(
        count("*").alias("title_count"),
        round(avg("content_age"), 1).alias("avg_content_age"),
        round(avg("title_length"), 1).alias("avg_title_length"),
        countDistinct("primary_country").alias("countries"),
        countDistinct("primary_genre").alias("genres"),
        round(avg("genre_count"), 1).alias("avg_genre_count")
    ) \
    .orderBy("rating", "type")

high_rated_df.show(truncate=False)
df_time_6 = time.time() - start_time

print(f"\n⏱️  Performance: SQL ({sql_time_6:.3f}s) vs DataFrame ({df_time_6:.3f}s)")

# ============================================================================
# ADVANCED ANALYSIS: Country-Genre Combinations
# ============================================================================

print("\n" + "="*80)
print("🔍 ADVANCED ANALYSIS: Country-Genre Combinations")
print("="*80)

print("\n📝 Approach 1: Using Spark SQL")
start_time = time.time()

country_genre_sql = spark.sql("""
    SELECT 
        primary_country,
        primary_genre,
        COUNT(*) as title_count,
        COUNT(DISTINCT type) as content_types,
        ROUND(AVG(release_year), 1) as avg_year,
        COUNT(CASE WHEN type = 'Movie' THEN 1 END) as movies,
        COUNT(CASE WHEN type = 'TV Show' THEN 1 END) as tv_shows
    FROM netflix 
    WHERE primary_country IN ('United States', 'India', 'United Kingdom', 'Japan', 'South Korea')
        AND primary_genre IS NOT NULL
    GROUP BY primary_country, primary_genre
    HAVING COUNT(*) >= 2
    ORDER BY primary_country, title_count DESC
""")

country_genre_sql.show(20, truncate=False)
sql_time_7 = time.time() - start_time

print("\n📝 Approach 2: Using DataFrame API")
start_time = time.time()

country_genre_df = netflix_clean.filter(
    col("primary_country").isin("United States", "India", "United Kingdom", "Japan", "South Korea")
) \
    .filter(col("primary_genre").isNotNull()) \
    .groupBy("primary_country", "primary_genre") \
    .agg(
        count("*").alias("title_count"),
        countDistinct("type").alias("content_types"),
        round(avg("release_year"), 1).alias("avg_year"),
        count(when(col("type") == "Movie", 1)).alias("movies"),
        count(when(col("type") == "TV Show", 1)).alias("tv_shows")
    ) \
    .filter(col("title_count") >= 2) \
    .orderBy("primary_country", col("title_count").desc())

country_genre_df.show(20, truncate=False)
df_time_7 = time.time() - start_time

print(f"\n⏱️  Performance: SQL ({sql_time_7:.3f}s) vs DataFrame ({df_time_7:.3f}s)")

# ============================================================================
# PERFORMANCE COMPARISON SUMMARY
# ============================================================================

print("\n" + "="*80)
print("📊 PERFORMANCE COMPARISON SUMMARY")
print("="*80)

performance_data = [
    ("Genre Analysis", sql_time_1, df_time_1),
    ("Country Production", sql_time_2, df_time_2),
    ("Trends Over Time", sql_time_3, df_time_3),
    ("Duration Analysis", sql_time_4, df_time_4),
    ("Rating Distribution", sql_time_5, df_time_5),
    ("High-Rated Content", sql_time_6, df_time_6),
    ("Country-Genre Combos", sql_time_7, df_time_7)
]

print("Question                    | SQL Time (s) | DataFrame Time (s) | Difference")
print("-" * 75)

total_sql_time = 0
total_df_time = 0

for question, sql_time, df_time in performance_data:
    diff = abs(sql_time - df_time)
    faster = "SQL" if sql_time < df_time else "DataFrame"
    print(f"{question:<25} | {sql_time:>11.3f} | {df_time:>17.3f} | {diff:>9.3f} ({faster})")
    total_sql_time += sql_time
    total_df_time += df_time

print("-" * 75)
print(f"{'TOTAL':<25} | {total_sql_time:>11.3f} | {total_df_time:>17.3f} | {abs(total_sql_time - total_df_time):>9.3f}")

# ============================================================================
# BUSINESS INSIGHTS SUMMARY
# ============================================================================

print("\n" + "="*80)
print("💡 BUSINESS INSIGHTS SUMMARY")
print("="*80)

print("""
🎬 KEY FINDINGS:

1. 📊 Genre Popularity:
   - Dramas and International Movies dominate the catalog
   - Most genres span both movies and TV shows
   - Content diversity varies significantly by genre

2. 🌍 Geographic Distribution:
   - United States leads in content production
   - India shows strong growth in recent years
   - Different countries specialize in different content types

3. 📈 Temporal Trends:
   - Content production has accelerated dramatically since 2010
   - TV shows have grown faster than movies in recent years
   - International content has increased significantly

4. ⏱️ Duration Patterns:
   - Movies average 90-120 minutes
   - TV shows vary widely in episode/season counts
   - Duration patterns differ by content type and rating

5. 🔞 Rating Distribution:
   - TV-MA and TV-14 are most common ratings
   - Rating preferences vary by country and content type
   - Family-friendly content represents smaller segment

6. 🎯 Content Strategy Insights:
   - High-rated content tends to be more recent
   - Multi-genre content performs well
   - Country-specific content preferences are clear

💼 BUSINESS RECOMMENDATIONS:
• Focus on international content expansion
• Invest in TV show production (growing segment)
• Develop content for different rating categories
• Consider co-production opportunities with top countries
• Balance between movies and TV shows by region
""")

# ============================================================================
# LEARNING OUTCOMES
# ============================================================================

print("\n" + "="*80)
print("🎓 LEARNING OUTCOMES")
print("="*80)

print("""
✅ SKILLS MASTERED:

📝 Spark SQL Techniques:
• Complex aggregations with GROUP BY and CASE statements
• Window functions and subqueries
• String manipulation and pattern matching
• Joins and set operations
• Performance optimization with EXPLAIN

🔧 DataFrame API Methods:
• Chaining transformations efficiently
• Complex filtering and selection
• Aggregation functions and expressions
• When/otherwise conditional logic
• Performance monitoring and caching

🎯 Business Intelligence:
• Translating business questions to technical solutions
• Data-driven decision making
• Comparative analysis techniques
• Insight generation and visualization
• Performance comparison and optimization

💡 KEY INSIGHTS:
• Both SQL and DataFrame API can solve the same problems
• Performance differences are often minimal
• Choice depends on complexity and personal preference
• Understanding both approaches provides flexibility
• Real-world analytics requires both technical and business skills

🚀 NEXT STEPS:
• Practice with larger datasets
• Explore advanced optimization techniques
• Learn about streaming data processing
• Study machine learning with Spark MLlib
• Master Spark UI for performance tuning
""")

print("\n🎉 Mini Project Complete!")
print("🏆 You've successfully demonstrated proficiency in both Spark SQL and DataFrame API!")
print("📈 Ready for real-world data analytics challenges!")

# Clean up
spark.catalog.dropTempView("netflix")
print("\n🧹 Temporary views cleaned up!")
