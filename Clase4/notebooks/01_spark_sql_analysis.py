"""
Spark SQL Data Analysis Examples
================================

This notebook demonstrates how to use Spark SQL with DataFrames for data analysis.
We'll use the Netflix dataset to explore various SQL operations including:
- Creating temporary views
- SELECT, WHERE, GROUP BY, JOIN operations
- Window functions and subqueries
- Complex analytical queries
"""

from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Netflix Spark SQL Analysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Session Created Successfully!")
print(f"Spark Version: {spark.version}")

# Load the Netflix data
netflix_df = spark.read.csv(
    "../data/netflix_data.csv",
    header=True,
    inferSchema=True,
    quote='"',
    escape='"'
)

print(f"📊 Dataset loaded with {netflix_df.count()} rows and {len(netflix_df.columns)} columns")
print("📋 DataFrame Schema:")
netflix_df.printSchema()
print("\n📄 Sample Data:")
netflix_df.show(5, truncate=False)

# Create a temporary view for SQL queries
netflix_df.createOrReplaceTempView("netflix_titles")
print("🔍 Temporary view 'netflix_titles' created successfully!")

# ============================================================================
# BASIC SQL OPERATIONS
# ============================================================================

print("\n" + "="*80)
print("📋 BASIC SQL OPERATIONS")
print("="*80)

# 1. SELECT with specific columns
print("\n1️⃣ SELECT specific columns:")
spark.sql("""
    SELECT 
        title, 
        type, 
        release_year, 
        rating,
        listed_in
    FROM netflix_titles 
    LIMIT 5
""").show(truncate=False)

# 2. WHERE clause with multiple conditions
print("\n2️⃣ WHERE clause with filtering:")
spark.sql("""
    SELECT 
        title, 
        type, 
        release_year, 
        country,
        rating
    FROM netflix_titles 
    WHERE type = 'Movie' 
        AND release_year >= 2020 
        AND rating IN ('PG-13', 'R')
    ORDER BY release_year DESC
""").show(truncate=False)

# 3. LIKE pattern matching
print("\n3️⃣ LIKE pattern matching:")
spark.sql("""
    SELECT 
        title, 
        type, 
        description
    FROM netflix_titles 
    WHERE title LIKE '%The%' 
        OR description LIKE '%mystery%'
    ORDER BY title
""").show(truncate=False)

# ============================================================================
# AGGREGATION AND GROUP BY
# ============================================================================

print("\n" + "="*80)
print("📊 AGGREGATION AND GROUP BY")
print("="*80)

# 1. Basic GROUP BY with COUNT
print("\n1️⃣ Content type distribution:")
spark.sql("""
    SELECT 
        type,
        COUNT(*) as total_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM netflix_titles), 2) as percentage
    FROM netflix_titles 
    GROUP BY type
    ORDER BY total_count DESC
""").show()

# 2. GROUP BY with multiple aggregations
print("\n2️⃣ Content by rating and type:")
spark.sql("""
    SELECT 
        type,
        rating,
        COUNT(*) as count,
        AVG(release_year) as avg_release_year,
        MIN(release_year) as earliest_year,
        MAX(release_year) as latest_year
    FROM netflix_titles 
    WHERE rating IS NOT NULL
    GROUP BY type, rating
    ORDER BY type, count DESC
""").show()

# 3. GROUP BY with HAVING clause
print("\n3️⃣ Countries with significant content (HAVING clause):")
spark.sql("""
    SELECT 
        country,
        COUNT(*) as total_titles,
        COUNT(DISTINCT type) as content_types
    FROM netflix_titles 
    WHERE country IS NOT NULL
    GROUP BY country
    HAVING COUNT(*) >= 2
    ORDER BY total_titles DESC
""").show(truncate=False)

# ============================================================================
# WINDOW FUNCTIONS
# ============================================================================

print("\n" + "="*80)
print("🪟 WINDOW FUNCTIONS")
print("="*80)

# 1. ROW_NUMBER() - Ranking content by release year
print("\n1️⃣ Ranking content by release year within each type:")
spark.sql("""
    SELECT 
        title,
        type,
        release_year,
        rating,
        ROW_NUMBER() OVER (PARTITION BY type ORDER BY release_year DESC) as rank_by_year
    FROM netflix_titles 
    WHERE release_year IS NOT NULL
    QUALIFY rank_by_year <= 3
    ORDER BY type, rank_by_year
""").show(truncate=False)

# 2. LAG/LEAD functions - Compare with previous/next entries
print("\n2️⃣ Year-over-year content addition analysis:")
spark.sql("""
    WITH yearly_content AS (
        SELECT 
            YEAR(date_added) as add_year,
            type,
            COUNT(*) as titles_added
        FROM netflix_titles 
        WHERE date_added IS NOT NULL
        GROUP BY YEAR(date_added), type
        ORDER BY add_year, type
    )
    SELECT 
        add_year,
        type,
        titles_added,
        LAG(titles_added, 1, 0) OVER (PARTITION BY type ORDER BY add_year) as previous_year,
        titles_added - LAG(titles_added, 1, 0) OVER (PARTITION BY type ORDER BY add_year) as year_over_year_change
    FROM yearly_content
    ORDER BY type, add_year
""").show()

# 3. Running totals and percentiles
print("\n3️⃣ Running totals and cumulative distribution:")
spark.sql("""
    SELECT 
        release_year,
        type,
        COUNT(*) as yearly_count,
        SUM(COUNT(*)) OVER (PARTITION BY type ORDER BY release_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_count,
        ROUND(
            SUM(COUNT(*)) OVER (PARTITION BY type ORDER BY release_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 / 
            SUM(COUNT(*)) OVER (PARTITION BY type), 2
        ) as cumulative_percentage
    FROM netflix_titles 
    WHERE release_year >= 2015
    GROUP BY release_year, type
    ORDER BY type, release_year
""").show(truncate=False)

# ============================================================================
# SUBQUERIES AND CTEs
# ============================================================================

print("\n" + "="*80)
print("🔗 SUBQUERIES AND COMMON TABLE EXPRESSIONS")
print("="*80)

# 1. Subquery in WHERE clause
print("\n1️⃣ Content from countries with above-average production:")
spark.sql("""
    SELECT 
        title,
        type,
        country,
        release_year
    FROM netflix_titles 
    WHERE country IN (
        SELECT country 
        FROM netflix_titles 
        WHERE country IS NOT NULL
        GROUP BY country 
        HAVING COUNT(*) > (
            SELECT AVG(title_count) 
            FROM (
                SELECT COUNT(*) as title_count 
                FROM netflix_titles 
                WHERE country IS NOT NULL
                GROUP BY country
            ) avg_counts
        )
    )
    ORDER BY country, title
""").show(truncate=False)

# 2. Complex CTE with multiple transformations
print("\n2️⃣ Genre analysis with CTEs:")
spark.sql("""
    WITH genre_expansion AS (
        SELECT 
            title,
            type,
            release_year,
            TRIM(genre) as genre
        FROM netflix_titles 
        LATERAL VIEW explode(split(listed_in, ',')) genres AS genre
    ),
    genre_stats AS (
        SELECT 
            genre,
            type,
            COUNT(*) as title_count,
            AVG(release_year) as avg_year
        FROM genre_expansion 
        GROUP BY genre, type
    ),
    genre_rankings AS (
        SELECT 
            genre,
            type,
            title_count,
            avg_year,
            ROW_NUMBER() OVER (PARTITION BY type ORDER BY title_count DESC) as rank_in_type,
            ROW_NUMBER() OVER (ORDER BY title_count DESC) as overall_rank
        FROM genre_stats
    )
    SELECT 
        genre,
        type,
        title_count,
        ROUND(avg_year, 1) as average_release_year,
        rank_in_type,
        overall_rank
    FROM genre_rankings
    WHERE rank_in_type <= 5
    ORDER BY type, rank_in_type
""").show(truncate=False)

# ============================================================================
# JOIN OPERATIONS
# ============================================================================

print("\n" + "="*80)
print("🔗 JOIN OPERATIONS")
print("="*80)

# Create a reference table for ratings
ratings_ref = spark.createDataFrame([
    ("TV-MA", "Mature Audience Only", "18+"),
    ("TV-14", "Parents Strongly Cautioned", "14+"),
    ("TV-PG", "Parental Guidance Suggested", "10+"),
    ("TV-G", "General Audience", "All Ages"),
    ("R", "Restricted", "17+"),
    ("PG-13", "Parents Strongly Cautioned", "13+"),
    ("PG", "Parental Guidance Suggested", "10+"),
    ("G", "General Audience", "All Ages")
], ["rating_code", "rating_description", "age_group"])

ratings_ref.createOrReplaceTempView("ratings_reference")

# 1. INNER JOIN
print("\n1️⃣ Content with rating descriptions (INNER JOIN):")
spark.sql("""
    SELECT 
        n.title,
        n.type,
        n.rating,
        r.rating_description,
        r.age_group
    FROM netflix_titles n
    INNER JOIN ratings_reference r ON n.rating = r.rating_code
    ORDER BY r.age_group, n.title
""").show(truncate=False)

# 2. LEFT JOIN
print("\n2️⃣ All content with rating info (LEFT JOIN):")
spark.sql("""
    SELECT 
        n.title,
        n.type,
        n.rating,
        COALESCE(r.rating_description, 'Unknown Rating') as rating_desc,
        COALESCE(r.age_group, 'Not Specified') as age_group
    FROM netflix_titles n
    LEFT JOIN ratings_reference r ON n.rating = r.rating_code
    ORDER BY n.rating, n.title
    LIMIT 10
""").show(truncate=False)

# ============================================================================
# CASE STATEMENTS AND CONDITIONAL LOGIC
# ============================================================================

print("\n" + "="*80)
print("🎯 CASE STATEMENTS AND CONDITIONAL LOGIC")
print("="*80)

print("\n1️⃣ Content categorization with CASE statements:")
spark.sql("""
    SELECT 
        title,
        type,
        release_year,
        rating,
        CASE 
            WHEN release_year >= 2020 THEN 'Recent Content'
            WHEN release_year >= 2015 AND release_year < 2020 THEN 'Modern Content'
            WHEN release_year >= 2000 AND release_year < 2015 THEN '21st Century Classic'
            ELSE 'Classic'
        END as content_era,
        CASE 
            WHEN rating IN ('TV-MA', 'R') THEN 'Adult Content'
            WHEN rating IN ('TV-14', 'PG-13') THEN 'Teen Content'
            WHEN rating IN ('TV-PG', 'PG', 'TV-G', 'G') THEN 'Family Content'
            ELSE 'Unrated'
        END as content_category
    FROM netflix_titles 
    ORDER BY release_year DESC
    LIMIT 10
""").show(truncate=False)

# ============================================================================
# PERFORMANCE OPTIMIZATION
# ============================================================================

print("\n" + "="*80)
print("⚡ PERFORMANCE OPTIMIZATION")
print("="*80)

# Cache the DataFrame for better performance
netflix_df.cache()
print("📦 DataFrame cached for better performance!")

# Show query plan
print("\n📋 Query Execution Plan:")
spark.sql("""
    SELECT 
        type, 
        rating, 
        COUNT(*) as count
    FROM netflix_titles 
    WHERE release_year >= 2020
    GROUP BY type, rating
    ORDER BY count DESC
""").explain(True)

# ============================================================================
# SUMMARY STATISTICS
# ============================================================================

print("\n" + "="*80)
print("📈 SUMMARY STATISTICS")
print("="*80)

print("\n📊 Dataset Summary:")
summary_stats = spark.sql("""
    SELECT 
        'Total Records' as metric,
        COUNT(*) as value
    FROM netflix_titles
    
    UNION ALL
    
    SELECT 
        'Unique Titles' as metric,
        COUNT(DISTINCT title) as value
    FROM netflix_titles
    
    UNION ALL
    
    SELECT 
        'Content Types' as metric,
        COUNT(DISTINCT type) as value
    FROM netflix_titles
    
    UNION ALL
    
    SELECT 
        'Countries Represented' as metric,
        COUNT(DISTINCT country) as value
    FROM netflix_titles
    
    UNION ALL
    
    SELECT 
        'Rating Categories' as metric,
        COUNT(DISTINCT rating) as value
    FROM netflix_titles
    
    UNION ALL
    
    SELECT 
        'Date Range' as metric,
        CONCAT(MIN(release_year), ' - ', MAX(release_year)) as value
    FROM netflix_titles
    WHERE release_year IS NOT NULL
""")

summary_stats.show(truncate=False)

print("\n✅ Spark SQL Analysis Complete!")
print("🎯 Key concepts demonstrated:")
print("   • Temporary views and basic SQL operations")
print("   • Aggregations, GROUP BY, and HAVING clauses")
print("   • Window functions (ROW_NUMBER, LAG, running totals)")
print("   • Subqueries and Common Table Expressions (CTEs)")
print("   • JOIN operations with multiple tables")
print("   • CASE statements and conditional logic")
print("   • Performance optimization with caching")

# Clean up
spark.catalog.dropTempView("netflix_titles")
spark.catalog.dropTempView("ratings_reference")
print("\n🧹 Temporary views cleaned up!")
