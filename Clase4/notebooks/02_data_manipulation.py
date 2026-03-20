"""
Structured Data Manipulation with Spark DataFrames
==================================================

This notebook demonstrates comprehensive DataFrame manipulation techniques including:
- Column operations and transformations
- Filtering and selecting data
- Aggregations and grouping
- Joins and unions
- Sorting and deduplication
- Advanced data cleaning and preparation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Netflix Data Manipulation") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Session Created Successfully!")

# Load data
netflix_df = spark.read.csv(
    "../data/netflix_data.csv",
    header=True,
    inferSchema=True,
    quote='"',
    escape='"'
)

print(f"📊 Dataset loaded: {netflix_df.count()} rows, {len(netflix_df.columns)} columns")
netflix_df.printSchema()

# ============================================================================
# COLUMN OPERATIONS
# ============================================================================

print("\n" + "="*80)
print("🔧 COLUMN OPERATIONS")
print("="*80)

# 1. Adding new columns with withColumn
print("\n1️⃣ Adding new columns:")

# Add year when content was added to Netflix
netflix_enhanced = netflix_df.withColumn(
    "year_added", 
    year(col("date_added"))
)

# Add content age (how old the content is when added)
netflix_enhanced = netflix_enhanced.withColumn(
    "content_age_when_added",
    col("year_added") - col("release_year")
)

# Add title length analysis
netflix_enhanced = netflix_enhanced.withColumn(
    "title_length",
    length(col("title"))
)

# Add description length
netflix_enhanced = netflix_enhanced.withColumn(
    "description_length",
    length(col("description"))
)

print("New columns added:")
netflix_enhanced.select("title", "year_added", "content_age_when_added", "title_length").show(5, truncate=False)

# 2. Column transformations with expressions
print("\n2️⃣ Column transformations:")

# Extract primary genre (first genre listed)
netflix_enhanced = netflix_enhanced.withColumn(
    "primary_genre",
    split(col("listed_in"), ",").getItem(0)
)

# Extract number of genres
netflix_enhanced = netflix_enhanced.withColumn(
    "genre_count",
    size(split(col("listed_in"), ","))
)

# Extract primary country
netflix_enhanced = netflix_enhanced.withColumn(
    "primary_country",
    split(col("country"), ",").getItem(0)
)

# Clean up whitespace in genre and country
netflix_enhanced = netflix_enhanced.withColumn(
    "primary_genre",
    trim(col("primary_genre"))
).withColumn(
    "primary_country",
    trim(col("primary_country"))
)

print("Transformed columns:")
netflix_enhanced.select("title", "listed_in", "primary_genre", "genre_count", "country", "primary_country").show(5, truncate=False)

# 3. Conditional columns with when/otherwise
print("\n3️⃣ Conditional columns:")

netflix_enhanced = netflix_enhanced.withColumn(
    "content_tier",
    when(col("rating").isin("TV-MA", "R"), "Premium")
    .when(col("rating").isin("TV-14", "PG-13"), "Standard")
    .when(col("rating").isin("TV-PG", "PG", "TV-G", "G"), "Family")
    .otherwise("Unrated")
)

netflix_enhanced = netflix_enhanced.withColumn(
    "content_vintage",
    when(col("release_year") >= 2020, "New")
    .when(col("release_year") >= 2015, "Modern")
    .when(col("release_year") >= 2000, "Contemporary")
    .otherwise("Classic")
)

netflix_enhanced = netflix_enhanced.withColumn(
    "has_director",
    when(col("director").isNotNull() & (col("director") != ""), True).otherwise(False)
)

print("Conditional columns:")
netflix_enhanced.select("title", "rating", "content_tier", "release_year", "content_vintage", "has_director").show(10, truncate=False)

# 4. Renaming and dropping columns
print("\n4️⃣ Column renaming and dropping:")

# Rename cast_field to cast_members
netflix_clean = netflix_enhanced.withColumnRenamed("cast_field", "cast_members")

# Drop original columns that are now redundant
columns_to_drop = ["date_added"]  # We have year_added
netflix_clean = netflix_clean.drop(*columns_to_drop)

print("Cleaned DataFrame schema:")
netflix_clean.printSchema()

# ============================================================================
# FILTERING AND SELECTING
# ============================================================================

print("\n" + "="*80)
print("🔍 FILTERING AND SELECTING")
print("="*80)

# 1. Basic filtering
print("\n1️⃣ Basic filtering examples:")

# Filter for recent movies
recent_movies = netflix_clean.filter(
    (col("type") == "Movie") & 
    (col("release_year") >= 2020)
)
print(f"Recent movies (2020+): {recent_movies.count()} titles")
recent_movies.select("title", "release_year", "rating", "primary_genre").show(5, truncate=False)

# Filter by content tier
premium_content = netflix_clean.filter(col("content_tier") == "Premium")
print(f"Premium content: {premium_content.count()} titles")

# Filter with multiple conditions
international_tv = netflix_clean.filter(
    (col("type") == "TV Show") &
    (col("primary_country") != "United States") &
    (col("content_age_when_added") >= 0)
)
print(f"International TV shows: {international_tv.count()} titles")

# 2. Complex filtering with string operations
print("\n2️⃣ Advanced filtering:")

# Content with "The" in title and longer descriptions
the_content = netflix_clean.filter(
    col("title").startswith("The") &
    (col("description_length") > 100)
)
print(f"Content starting with 'The' and long descriptions: {the_content.count()} titles")

# Multi-country productions
multi_country = netflix_clean.filter(
    col("country").contains(",") &
    (col("genre_count") > 1)
)
print(f"Multi-country, multi-genre content: {multi_country.count()} titles")

# 3. Selecting specific columns
print("\n3️⃣ Column selection:")

# Select specific columns for analysis
analysis_df = netflix_clean.select(
    "show_id", "title", "type", "primary_genre", "primary_country",
    "release_year", "rating", "content_tier", "content_vintage",
    "title_length", "description_length", "genre_count"
)

print("Analysis DataFrame:")
analysis_df.show(5, truncate=False)

# Select with expressions
content_summary = netflix_clean.select(
    col("title"),
    col("type"),
    concat_ws(" - ", col("primary_country"), col("primary_genre")).alias("content_origin"),
    concat(col("title"), " (", col("release_year"), ")").alias("title_with_year")
)
content_summary.show(5, truncate=False)

# ============================================================================
# AGGREGATIONS AND GROUPING
# ============================================================================

print("\n" + "="*80)
print("📊 AGGREGATIONS AND GROUPING")
print("="*80)

# 1. Basic aggregations
print("\n1️⃣ Basic aggregations:")

# Content statistics by type
type_stats = netflix_clean.groupBy("type").agg(
    count("*").alias("total_titles"),
    avg("release_year").alias("avg_release_year"),
    min("release_year").alias("earliest_year"),
    max("release_year").alias("latest_year"),
    avg("title_length").alias("avg_title_length"),
    avg("description_length").alias("avg_description_length")
)

print("Content statistics by type:")
type_stats.show(truncate=False)

# Content by rating tier
tier_stats = netflix_clean.groupBy("content_tier").agg(
    count("*").alias("title_count"),
    countDistinct("primary_country").alias("countries"),
    countDistinct("primary_genre").alias("genres"),
    round(avg("release_year"), 1).alias("avg_release_year")
).orderBy(col("title_count").desc())

print("Content by rating tier:")
tier_stats.show(truncate=False)

# 2. Multiple aggregations with aliasing
print("\n2️⃣ Multiple aggregations:")

# Comprehensive genre analysis
genre_analysis = netflix_clean.groupBy("primary_genre").agg(
    count("*").alias("total_titles"),
    count(when(col("type") == "Movie", True)).alias("movie_count"),
    count(when(col("type") == "TV Show", True)).alias("tv_show_count"),
    round(avg("release_year"), 1).alias("avg_release_year"),
    round(avg("title_length"), 1).alias("avg_title_length"),
    countDistinct("primary_country").alias("countries_represented"),
    count(when(col("has_director") == True, True)).alias("with_director")
).orderBy(col("total_titles").desc())

print("Genre analysis:")
genre_analysis.show(10, truncate=False)

# 3. Rolling aggregations with window functions
print("\n3️⃣ Window function aggregations:")

window_spec = Window.partitionBy("type").orderBy(col("release_year").desc())

# Add rolling statistics
netflix_with_rolling = netflix_clean.withColumn(
    "rank_by_year",
    row_number().over(window_spec)
).withColumn(
    "content_count_by_type",
    count("*").over(Window.partitionBy("type"))
).withColumn(
    "yearly_content_count",
    count("*").over(Window.partitionBy("type", "release_year"))
)

print("Rolling statistics:")
netflix_with_rolling.select(
    "title", "type", "release_year", "rank_by_year", 
    "content_count_by_type", "yearly_content_count"
).filter(col("rank_by_year") <= 3).show(10, truncate=False)

# ============================================================================
# JOINS AND UNIONS
# ============================================================================

print("\n" + "="*80)
print("🔗 JOINS AND UNIONS")
print("="*80)

# 1. Self-join for content comparison
print("\n1️⃣ Self-join example:")

# Find content from the same year and country
content_comparison = netflix_clean.alias("df1").join(
    netflix_clean.alias("df2"),
    (col("df1.release_year") == col("df2.release_year")) &
    (col("df1.primary_country") == col("df2.primary_country")) &
    (col("df1.show_id") != col("df2.show_id")),
    "inner"
).select(
    col("df1.title").alias("title1"),
    col("df1.type").alias("type1"),
    col("df2.title").alias("title2"),
    col("df2.type").alias("type2"),
    col("df1.release_year"),
    col("df1.primary_country")
).filter(col("df1.type") != col("df2.type"))

print("Content from same year and country but different types:")
content_comparison.show(10, truncate=False)

# 2. Join with aggregated data
print("\n2️⃣ Join with aggregated data:")

# Create country statistics
country_stats = netflix_clean.groupBy("primary_country").agg(
    count("*").alias("country_titles"),
    countDistinct("primary_genre").alias("genre_diversity"),
    avg("release_year").alias("avg_year")
)

# Join back to original data
netflix_with_country_stats = netflix_clean.join(
    country_stats,
    "primary_country",
    "left"
)

print("Content with country statistics:")
netflix_with_country_stats.select(
    "title", "primary_country", "country_titles", "genre_diversity"
).show(10, truncate=False)

# 3. Union operations
print("\n3️⃣ Union operations:")

# Create separate DataFrames for movies and TV shows
movies_df = netflix_clean.filter(col("type") == "Movie").select(
    "title", "release_year", "rating", "primary_genre", "primary_country"
).withColumn("content_category", lit("Cinema"))

tv_shows_df = netflix_clean.filter(col("type") == "TV Show").select(
    "title", "release_year", "rating", "primary_genre", "primary_country"
).withColumn("content_category", lit("Television"))

# Union the DataFrames
unified_content = movies_df.unionByName(tv_shows_df)

print("Unified content DataFrame:")
unified_content.show(10, truncate=False)

# ============================================================================
# SORTING AND DEDUPLICATION
# ============================================================================

print("\n" + "="*80)
print("🔄 SORTING AND DEDUPLICATION")
print("="*80)

# 1. Multiple column sorting
print("\n1️⃣ Multi-column sorting:")

sorted_content = netflix_clean.orderBy(
    col("content_tier").desc(),
    col("release_year").desc(),
    col("title_length").asc()
)

print("Content sorted by tier, year, and title length:")
sorted_content.select("title", "content_tier", "release_year", "title_length").show(10, truncate=False)

# 2. Deduplication
print("\n2️⃣ Deduplication examples:")

# Remove exact duplicates
unique_content = netflix_clean.dropDuplicates()
print(f"Original: {netflix_clean.count()}, After deduplication: {unique_content.count()}")

# Remove duplicates based on specific columns
unique_titles = netflix_clean.dropDuplicates(["title"])
print(f"Unique titles: {unique_titles.count()}")

# 3. Window-based deduplication
print("\n3️⃣ Advanced deduplication with window functions:")

window_spec = Window.partitionBy("title").orderBy(col("release_year").desc())

# Keep the most recent version of each title
deduped_content = netflix_clean.withColumn(
    "row_num", 
    row_number().over(window_spec)
).filter(col("row_num") == 1).drop("row_num")

print("Deduplicated content (keeping most recent):")
deduped_content.select("title", "release_year", "type").show(10, truncate=False)

# ============================================================================
# DATA CLEANING AND VALIDATION
# ============================================================================

print("\n" + "="*80)
print("🧹 DATA CLEANING AND VALIDATION")
print("="*80)

# 1. Handle missing values
print("\n1️⃣ Missing value analysis:")

# Count missing values in each column
missing_counts = netflix_clean.select([
    count(when(col(c).isNull() | (col(c) == ""), c)).alias(c)
    for c in netflix_clean.columns
])

print("Missing value counts:")
missing_counts.show(truncate=False)

# Fill missing values
cleaned_df = netflix_clean.fillna({
    "director": "Unknown",
    "cast_members": "Not Specified",
    "country": "Unknown",
    "rating": "Not Rated",
    "content_age_when_added": 0,
    "title_length": 0,
    "description_length": 0
})

print("DataFrame after filling missing values:")
cleaned_df.select("title", "director", "cast_members", "country", "rating").show(5, truncate=False)

# 2. Data validation and quality checks
print("\n2️⃣ Data validation:")

# Validate release years
invalid_years = cleaned_df.filter(
    (col("release_year") < 1900) | (col("release_year") > 2030)
)
print(f"Invalid release years: {invalid_years.count()}")

# Validate content age
invalid_age = cleaned_df.filter(col("content_age_when_added") < -10)
print(f"Suspicious content age values: {invalid_age.count()}")

# 3. Data type corrections
print("\n3️⃣ Data type corrections:")

# Ensure proper data types
final_df = cleaned_df.withColumn(
    "release_year",
    col("release_year").cast(IntegerType())
).withColumn(
    "title_length",
    col("title_length").cast(IntegerType())
).withColumn(
    "description_length",
    col("description_length").cast(IntegerType())
).withColumn(
    "genre_count",
    col("genre_count").cast(IntegerType())
)

print("Final DataFrame schema:")
final_df.printSchema()

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*80)
print("📋 MANIPULATION SUMMARY")
print("="*80)

print(f"🎯 Final DataFrame: {final_df.count()} rows, {len(final_df.columns)} columns")
print("\n📊 Column Categories:")
print("   • Original columns: show_id, type, title, director, cast_members, country, release_year, rating, duration, listed_in, description")
print("   • Enhanced columns: year_added, content_age_when_added, title_length, description_length")
print("   • Derived columns: primary_genre, genre_count, primary_country, content_tier, content_vintage, has_director")

print("\n🔧 Operations Demonstrated:")
print("   • Column operations: withColumn, withColumnRenamed, drop")
print("   • Transformations: split, trim, length, concat, when/otherwise")
print("   • Filtering: filter, where, startswith, contains")
print("   • Selection: select, with expressions")
print("   • Aggregations: groupBy, agg, count, avg, min, max")
print("   • Window functions: row_number, partitionBy, orderBy")
print("   • Joins: join, self-join, unionByName")
print("   • Sorting: orderBy with multiple columns")
print("   • Deduplication: dropDuplicates, window-based dedup")
print("   • Data cleaning: fillna, validation, type casting")

print("\n✅ Data Manipulation Complete!")

# Save the cleaned dataset for future use
final_df.write.mode("overwrite").parquet("../data/netflix_cleaned.parquet")
print("\n💾 Cleaned dataset saved as Parquet file!")
