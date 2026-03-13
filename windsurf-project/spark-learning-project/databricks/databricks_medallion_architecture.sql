-- ===================================================================
-- Databricks Medallion Architecture: Bronze-Silver-Gold
-- Using Airbnb Public Dataset
-- ===================================================================

-- ===================================================================
-- SETUP: Create Databases for Each Layer
-- ===================================================================

-- Create databases for medallion architecture
CREATE DATABASE IF NOT EXISTS bronze_layer COMMENT 'Raw data landing zone';
CREATE DATABASE IF NOT EXISTS silver_layer COMMENT 'Cleaned and validated data';
CREATE DATABASE IF NOT EXISTS gold_layer COMMENT 'Business-ready aggregated data';

-- Use bronze layer for initial table creation
USE bronze_layer;

-- ===================================================================
-- BRONZE LAYER: Raw Data Tables
-- ===================================================================

-- Table for raw Airbnb listings data
CREATE OR REPLACE TABLE bronze_airbnb_listings (
    id STRING,
    name STRING,
    host_id STRING,
    host_name STRING,
    neighbourhood_group STRING,
    neighbourhood STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    room_type STRING,
    price STRING,
    minimum_nights STRING,
    number_of_reviews STRING,
    last_review STRING,
    reviews_per_month STRING,
    calculated_host_listings_count STRING,
    availability_365 STRING,
    raw_file_path STRING,
    ingestion_timestamp TIMESTAMP,
    source_system STRING
) USING DELTA
COMMENT 'Raw Airbnb listings data from public dataset'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- Table for raw Airbnb reviews data
CREATE OR REPLACE TABLE bronze_airbnb_reviews (
    listing_id STRING,
    id STRING,
    date STRING,
    reviewer_id STRING,
    reviewer_name STRING,
    comments STRING,
    raw_file_path STRING,
    ingestion_timestamp TIMESTAMP,
    source_system STRING
) USING DELTA
COMMENT 'Raw Airbnb reviews data from public dataset'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- Table for raw Airbnb calendar data
CREATE OR REPLACE TABLE bronze_airbnb_calendar (
    listing_id STRING,
    date STRING,
    available STRING,
    price STRING,
    adjusted_price STRING,
    minimum_nights STRING,
    maximum_nights STRING,
    raw_file_path STRING,
    ingestion_timestamp TIMESTAMP,
    source_system STRING
) USING DELTA
COMMENT 'Raw Airbnb calendar data from public dataset'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- ===================================================================
-- SILVER LAYER: Cleaned and Validated Data
-- ===================================================================

USE silver_layer;

-- Cleaned listings table
CREATE OR REPLACE TABLE silver_airbnb_listings (
    id BIGINT,
    name STRING,
    host_id BIGINT,
    host_name STRING,
    neighbourhood_group STRING,
    neighbourhood STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    room_type STRING,
    price DECIMAL(10,2),
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month DECIMAL(5,2),
    calculated_host_listings_count INT,
    availability_365 INT,
    is_active BOOLEAN,
    price_category STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA
COMMENT 'Cleaned and validated Airbnb listings data'
PARTITIONED BY (neighbourhood_group)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Cleaned reviews table
CREATE OR REPLACE TABLE silver_airbnb_reviews (
    listing_id BIGINT,
    review_id BIGINT,
    review_date DATE,
    reviewer_id BIGINT,
    reviewer_name STRING,
    comments STRING,
    sentiment_score DOUBLE,
    review_length INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA
COMMENT 'Cleaned and enriched Airbnb reviews data'
PARTITIONED BY (review_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Cleaned calendar table
CREATE OR REPLACE TABLE silver_airbnb_calendar (
    listing_id BIGINT,
    calendar_date DATE,
    available BOOLEAN,
    price DECIMAL(10,2),
    adjusted_price DECIMAL(10,2),
    minimum_nights INT,
    maximum_nights INT,
    is_weekend BOOLEAN,
    season STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA
COMMENT 'Cleaned and enriched Airbnb calendar data'
PARTITIONED BY (calendar_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ===================================================================
-- GOLD LAYER: Business-Ready Aggregated Data
-- ===================================================================

USE gold_layer;

-- Aggregate listings analytics table
CREATE OR REPLACE TABLE gold_listings_analytics (
    neighbourhood_group STRING,
    neighbourhood STRING,
    room_type STRING,
    total_listings INT,
    avg_price DECIMAL(10,2),
    median_price DECIMAL(10,2),
    min_price DECIMAL(10,2),
    max_price DECIMAL(10,2),
    avg_minimum_nights DECIMAL(5,2),
    avg_availability DECIMAL(5,2),
    total_reviews INT,
    avg_reviews_per_month DECIMAL(5,2),
    active_listings INT,
    price_distribution MAP<STRING, INT>,
    last_updated TIMESTAMP
) USING DELTA
COMMENT 'Aggregated listings analytics by location and room type'
PARTITIONED BY (neighbourhood_group)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Host analytics table
CREATE OR REPLACE TABLE gold_host_analytics (
    host_id BIGINT,
    host_name STRING,
    total_listings INT,
    total_reviews INT,
    avg_listing_price DECIMAL(10,2),
    avg_rating DECIMAL(3,2),
    total_revenue_estimate DECIMAL(15,2),
    host_tier STRING,
    superhost_candidate BOOLEAN,
    years_active INT,
    last_updated TIMESTAMP
) USING DELTA
COMMENT 'Host performance analytics and metrics'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Time series analytics table
CREATE OR REPLACE TABLE gold_time_series_analytics (
    date DATE,
    year INT,
    month INT,
    quarter INT,
    day_of_week INT,
    is_weekend BOOLEAN,
    season STRING,
    total_active_listings INT,
    avg_price DECIMAL(10,2),
    total_bookings INT,
    occupancy_rate DECIMAL(5,2),
    avg_reviews_per_day DECIMAL(5,2),
    new_listings INT,
    price_volatility DECIMAL(5,2),
    last_updated TIMESTAMP
) USING DELTA
COMMENT 'Time series analytics for Airbnb market trends'
PARTITIONED BY (year, month)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Market insights table
CREATE OR REPLACE TABLE gold_market_insights (
    metric_name STRING,
    metric_value DECIMAL(15,2),
    metric_dimension STRING,
    comparison_period STRING,
    change_percentage DECIMAL(5,2),
    trend_direction STRING,
    insight_level STRING,
    generated_at TIMESTAMP
) USING DELTA
COMMENT 'Business insights and KPIs for Airbnb market analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ===================================================================
-- VIEWS FOR BUSINESS USERS
-- ===================================================================

-- View for top performing neighborhoods
CREATE OR REPLACE VIEW gold_top_neighborhoods AS
SELECT 
    neighbourhood_group,
    neighbourhood,
    total_listings,
    avg_price,
    total_reviews,
    avg_reviews_per_month,
    active_listings,
    RANK() OVER (PARTITION BY neighbourhood_group ORDER BY avg_price DESC) as price_rank,
    RANK() OVER (PARTITION BY neighbourhood_group ORDER BY total_reviews DESC) as popularity_rank
FROM gold_listings_analytics
WHERE total_listings >= 10
ORDER BY neighbourhood_group, avg_price DESC;

-- View for host performance dashboard
CREATE OR REPLACE VIEW gold_host_performance_dashboard AS
SELECT 
    host_id,
    host_name,
    host_tier,
    total_listings,
    total_reviews,
    avg_listing_price,
    total_revenue_estimate,
    CASE 
        WHEN total_reviews > 100 AND avg_rating >= 4.5 THEN 'Top Performer'
        WHEN total_reviews > 50 AND avg_rating >= 4.0 THEN 'Good Performer'
        WHEN total_reviews > 10 THEN 'Emerging'
        ELSE 'New'
    END as performance_category
FROM gold_host_analytics
WHERE total_listings >= 1
ORDER BY total_revenue_estimate DESC;

-- View for seasonal trends
CREATE OR REPLACE VIEW gold_seasonal_trends AS
SELECT 
    season,
    year,
    AVG(avg_price) as seasonal_avg_price,
    AVG(occupancy_rate) as seasonal_occupancy,
    SUM(total_bookings) as seasonal_bookings,
    COUNT(DISTINCT date) as days_in_season
FROM gold_time_series_analytics
GROUP BY season, year
ORDER BY year, 
    CASE season 
        WHEN 'Spring' THEN 1
        WHEN 'Summer' THEN 2
        WHEN 'Fall' THEN 3
        WHEN 'Winter' THEN 4
    END;

-- ===================================================================
-- STORED PROCEDURES FOR DATA PROCESSING
-- ===================================================================

-- Procedure to process bronze to silver layer
CREATE OR REPLACE PROCEDURE bronze_to_silver_listings()
LANGUAGE PYTHON
AS $$
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Read from bronze layer
bronze_df = spark.read.table("bronze_layer.bronze_airbnb_listings")

# Data cleaning and transformation
silver_df = bronze_df.filter(
    F.col("id").isNotNull() & 
    F.col("price").isNotNull()
).withColumn("id", F.col("id").cast("bigint")) \
 .withColumn("host_id", F.col("host_id").cast("bigint")) \
 .withColumn("price", F.regexp_replace(F.col("price"), r'[$,]', '').cast("decimal(10,2)")) \
 .withColumn("minimum_nights", F.col("minimum_nights").cast("int")) \
 .withColumn("number_of_reviews", F.col("number_of_reviews").cast("int")) \
 .withColumn("reviews_per_month", F.col("reviews_per_month").cast("decimal(5,2)")) \
 .withColumn("calculated_host_listings_count", F.col("calculated_host_listings_count").cast("int")) \
 .withColumn("availability_365", F.col("availability_365").cast("int")) \
 .withColumn("last_review", F.to_date("last_review", "yyyy-MM-dd")) \
 .withColumn("is_active", F.col("availability_365") > 0) \
 .withColumn("price_category", 
    F.when(F.col("price") < 50, "Budget")
     .when(F.col("price") < 150, "Mid-range")
     .when(F.col("price") < 300, "Premium")
     .otherwise("Luxury")
 ) \
 .withColumn("created_at", F.current_timestamp()) \
 .withColumn("updated_at", F.current_timestamp()) \
 .drop("raw_file_path", "ingestion_timestamp", "source_system")

# Write to silver layer with merge logic (SCD Type 2)
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_layer.silver_airbnb_listings")

# Log completion (no return statement needed for procedures)
print("Bronze to Silver transformation completed for listings")
$$;

-- Procedure to process reviews from bronze to silver
CREATE OR REPLACE PROCEDURE bronze_to_silver_reviews()
LANGUAGE PYTHON
AS $$
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Read from bronze layer
bronze_df = spark.read.table("bronze_layer.bronze_airbnb_reviews")

# Data cleaning and sentiment analysis
silver_df = bronze_df.filter(
    F.col("listing_id").isNotNull() & 
    F.col("id").isNotNull()
).withColumn("listing_id", F.col("listing_id").cast("bigint")) \
 .withColumn("review_id", F.col("id").cast("bigint")) \
 .withColumn("review_date", F.to_date("date", "yyyy-MM-dd")) \
 .withColumn("reviewer_id", F.col("reviewer_id").cast("bigint")) \
 .withColumn("review_length", F.length(F.col("comments"))) \
 .withColumn("sentiment_score", 
    F.when(F.col("comments").rlike("(great|excellent|amazing|wonderful|perfect)"), 0.8)
     .when(F.col("comments").rlike("(good|nice|clean|comfortable)"), 0.6)
     .when(F.col("comments").rlike("(okay|fine|average)"), 0.4)
     .when(F.col("comments").rlike("(bad|terrible|awful|disappointing)"), 0.2)
     .otherwise(0.5)
 ) \
 .withColumn("created_at", F.current_timestamp()) \
 .withColumn("updated_at", F.current_timestamp()) \
 .drop("id", "date", "raw_file_path", "ingestion_timestamp", "source_system")

# Write to silver layer
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_layer.silver_airbnb_reviews")

# Log completion (no return statement needed for procedures)
print("Bronze to Silver transformation completed for reviews")
$$;

-- Procedure to populate gold layer aggregations
CREATE OR REPLACE PROCEDURE populate_gold_analytics()
LANGUAGE PYTHON
AS $$
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Read from silver layer
listings_df = spark.read.table("silver_layer.silver_airbnb_listings")
reviews_df = spark.read.table("silver_layer.silver_airbnb_reviews")

# Listings analytics
listings_analytics = listings_df.groupBy(
    "neighbourhood_group", "neighbourhood", "room_type"
).agg(
    F.count("*").alias("total_listings"),
    F.avg("price").alias("avg_price"),
    F.expr("percentile_approx(price, 0.5)").alias("median_price"),
    F.min("price").alias("min_price"),
    F.max("price").alias("max_price"),
    F.avg("minimum_nights").alias("avg_minimum_nights"),
    F.avg("availability_365").alias("avg_availability"),
    F.sum("number_of_reviews").alias("total_reviews"),
    F.avg("reviews_per_month").alias("avg_reviews_per_month"),
    F.sum(F.when(F.col("is_active") == True, 1).otherwise(0)).alias("active_listings")
).withColumn("price_distribution", 
    F.map_from_arrays(
        F.array(F.lit("Budget"), F.lit("Mid-range"), F.lit("Premium"), F.lit("Luxury")),
        F.array(
            F.sum(F.when(F.col("price_category") == "Budget", 1).otherwise(0)),
            F.sum(F.when(F.col("price_category") == "Mid-range", 1).otherwise(0)),
            F.sum(F.when(F.col("price_category") == "Premium", 1).otherwise(0)),
            F.sum(F.when(F.col("price_category") == "Luxury", 1).otherwise(0))
        )
    )
).withColumn("last_updated", F.current_timestamp())

# Write to gold layer
listings_analytics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_layer.gold_listings_analytics")

# Host analytics
host_analytics = listings_df.groupBy("host_id", "host_name").agg(
    F.count("*").alias("total_listings"),
    F.sum("number_of_reviews").alias("total_reviews"),
    F.avg("price").alias("avg_listing_price"),
    F.avg(F.col("reviews_per_month")).alias("avg_rating"),
    F.sum(F.col("price") * F.col("availability_365") * 0.7).alias("total_revenue_estimate")  # Rough revenue estimate
).withColumn("host_tier",
    F.when(F.col("total_listings") >= 10, "Professional")
     .when(F.col("total_listings") >= 3, "Experienced")
     .otherwise("Casual")
).withColumn("superhost_candidate", 
    (F.col("total_reviews") > 50) & (F.col("avg_rating") >= 4.8)
).withColumn("years_active", F.floor(F.datediff(F.current_date(), F.min("last_review")) / 365))
 .withColumn("last_updated", F.current_timestamp())

host_analytics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_layer.gold_host_analytics")

# Log completion (no return statement needed for procedures)
print("Gold layer analytics populated successfully")
$$;

-- ===================================================================
-- SAMPLE DATA INSERTION (for testing)
-- ===================================================================

-- Insert sample data into bronze layer
INSERT INTO bronze_layer.bronze_airbnb_listings VALUES
('1234', 'Cozy Studio in Manhattan', '5678', 'John Doe', 'Manhattan', 'Harlem', 40.8116, -73.9465, 'Entire home/apt', '150.00', '3', '45', '2023-12-01', '2.5', '2', '200', '/path/to/file1.csv', CURRENT_TIMESTAMP(), 'airbnb_api'),
('5678', 'Luxury Brooklyn Loft', '9012', 'Jane Smith', 'Brooklyn', 'Williamsburg', 40.7081, -73.9571, 'Entire home/apt', '250.00', '5', '120', '2023-11-15', '4.2', '1', '350', '/path/to/file2.csv', CURRENT_TIMESTAMP(), 'airbnb_api');

INSERT INTO bronze_layer.bronze_airbnb_reviews VALUES
('1234', '1', '2023-12-01', '9999', 'Alice', 'Great place to stay! Very clean and comfortable.', '/path/to/review1.csv', CURRENT_TIMESTAMP(), 'airbnb_api'),
('1234', '2', '2023-11-28', '8888', 'Bob', 'Amazing location and host was very responsive.', '/path/to/review2.csv', CURRENT_TIMESTAMP(), 'airbnb_api');

-- ===================================================================
-- SAMPLE QUERIES FOR VALIDATION
-- ===================================================================

-- Query to validate bronze layer
SELECT * FROM bronze_layer.bronze_airbnb_listings LIMIT 5;

-- Query to validate silver layer after transformation
SELECT 
    neighbourhood_group,
    COUNT(*) as total_listings,
    AVG(price) as avg_price,
    AVG(reviews_per_month) as avg_reviews
FROM silver_layer.silver_airbnb_listings
GROUP BY neighbourhood_group
ORDER BY avg_price DESC;

-- Query to validate gold layer aggregations
SELECT 
    neighbourhood_group,
    neighbourhood,
    total_listings,
    avg_price,
    total_reviews
FROM gold_layer.gold_listings_analytics
WHERE total_listings > 0
ORDER BY avg_price DESC
LIMIT 10;
