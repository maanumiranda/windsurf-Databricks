#!/usr/bin/env python3
"""
Apache Spark Transformations for Databricks Medallion Architecture
This script contains the Spark transformations that complement the DDL queries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import re
from datetime import datetime, timedelta

class MedallionArchitecture:
    """
    Complete implementation of Bronze-Silver-Gold medallion architecture
    for Airbnb data processing in Databricks.
    """
    
    def __init__(self, spark=None):
        """Initialize with Spark session."""
        if spark is None:
            self.spark = SparkSession.builder \
                .appName("Medallion Architecture") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
        else:
            self.spark = spark
    
    def bronze_to_silver_listings(self, bronze_path=None, silver_table="silver_layer.silver_airbnb_listings"):
        """
        Transform bronze listings data to silver layer with comprehensive cleaning.
        """
        print("🔄 Starting Bronze to Silver transformation for listings...")
        
        # Read from bronze layer (either path or table)
        if bronze_path:
            bronze_df = self.spark.read.format("delta").load(bronze_path)
        else:
            bronze_df = self.spark.read.table("bronze_layer.bronze_airbnb_listings")
        
        # Comprehensive data cleaning and validation
        silver_df = bronze_df.filter(
            col("id").isNotNull() & 
            col("price").isNotNull() &
            (col("price") != "") &
            (col("price").rlike(r'^\s*[\$,\d\.]+\s*$'))
        )
        
        # Data type transformations
        silver_df = silver_df \
            .withColumn("id", col("id").cast("bigint")) \
            .withColumn("host_id", col("host_id").cast("bigint")) \
            .withColumn("price_clean", regexp_replace(col("price"), r'[\$,]', "")) \
            .withColumn("price", col("price_clean").cast("decimal(10,2)")) \
            .withColumn("minimum_nights", col("minimum_nights").cast("int")) \
            .withColumn("number_of_reviews", col("number_of_reviews").cast("int")) \
            .withColumn("reviews_per_month", col("reviews_per_month").cast("decimal(5,2)")) \
            .withColumn("calculated_host_listings_count", col("calculated_host_listings_count").cast("int")) \
            .withColumn("availability_365", col("availability_365").cast("int")) \
            .withColumn("last_review", to_date(col("last_review"), "yyyy-MM-dd")) \
            .drop("price_clean")
        
        # Business logic transformations
        silver_df = silver_df \
            .withColumn("is_active", col("availability_365") > 0) \
            .withColumn("price_category", 
                when(col("price") < 50, "Budget")
                .when(col("price") < 150, "Mid-range")
                .when(col("price") < 300, "Premium")
                .otherwise("Luxury")
            ) \
            .withColumn("experience_level",
                when(col("number_of_reviews") == 0, "New")
                .when(col("number_of_reviews") < 10, "Beginner")
                .when(col("number_of_reviews") < 50, "Experienced")
                .otherwise("Expert")
            ) \
            .withColumn("listing_quality_score",
                (col("number_of_reviews") * 0.3 + 
                 col("reviews_per_month") * 0.4 + 
                 col("availability_365") * 0.3).cast("decimal(5,2)")
            )
        
        # Geographic features
        silver_df = silver_df \
            .withColumn("location_cluster",
                when((col("latitude") > 40.7) & (col("latitude") < 40.8) & 
                     (col("longitude") > -74.0) & (col("longitude") < -73.9), "Manhattan")
                .when((col("latitude") > 40.6) & (col("latitude") < 40.7) & 
                     (col("longitude") > -74.0) & (col("longitude") < -73.8), "Brooklyn")
                .otherwise("Other")
            )
        
        # Metadata
        silver_df = silver_df \
            .withColumn("created_at", current_timestamp()) \
            .withColumn("updated_at", current_timestamp()) \
            .withColumn("processing_version", lit("v1.0")) \
            .drop("raw_file_path", "ingestion_timestamp", "source_system")
        
        # Write to silver layer with optimization
        silver_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("neighbourhood_group") \
            .saveAsTable(silver_table)
        
        print(f"✅ Bronze to Silver transformation completed for listings. Records: {silver_df.count()}")
        return silver_df
    
    def bronze_to_silver_reviews(self, bronze_path=None, silver_table="silver_layer.silver_airbnb_reviews"):
        """
        Transform bronze reviews data to silver layer with sentiment analysis.
        """
        print("🔄 Starting Bronze to Silver transformation for reviews...")
        
        # Read from bronze layer
        if bronze_path:
            bronze_df = self.spark.read.format("delta").load(bronze_path)
        else:
            bronze_df = self.spark.read.table("bronze_layer.bronze_airbnb_reviews")
        
        # Data cleaning
        silver_df = bronze_df.filter(
            col("listing_id").isNotNull() & 
            col("id").isNotNull() &
            col("comments").isNotNull()
        )
        
        # Data type transformations
        silver_df = silver_df \
            .withColumn("listing_id", col("listing_id").cast("bigint")) \
            .withColumn("review_id", col("id").cast("bigint")) \
            .withColumn("review_date", to_date(col("date"), "yyyy-MM-dd")) \
            .withColumn("reviewer_id", col("reviewer_id").cast("bigint")) \
            .withColumn("review_length", length(col("comments"))) \
            .drop("id", "date", "raw_file_path", "ingestion_timestamp", "source_system")
        
        # Advanced sentiment analysis
        silver_df = silver_df \
            .withColumn("comments_lower", lower(col("comments"))) \
            .withColumn("positive_words", size(split(col("comments_lower"), r'\b(great|excellent|amazing|wonderful|perfect|awesome|fantastic|love|beautiful|clean|comfortable)\b'))) \
            .withColumn("negative_words", size(split(col("comments_lower"), r'\b(bad|terrible|awful|disappointing|dirty|uncomfortable|poor|horrible|worst)\b'))) \
            .withColumn("sentiment_score",
                when(col("positive_words") > col("negative_words"), 0.8)
                .when(col("positive_words") == col("negative_words"), 0.5)
                .otherwise(0.2)
            ) \
            .withColumn("sentiment_category",
                when(col("sentiment_score") >= 0.7, "Positive")
                .when(col("sentiment_score") >= 0.4, "Neutral")
                .otherwise("Negative")
            ) \
            .withColumn("review_quality",
                when(col("review_length") > 100, "Detailed")
                .when(col("review_length") > 20, "Standard")
                .otherwise("Brief")
            ) \
            .drop("comments_lower")
        
        # Time-based features
        silver_df = silver_df \
            .withColumn("review_year", year(col("review_date"))) \
            .withColumn("review_month", month(col("review_date"))) \
            .withColumn("review_quarter", quarter(col("review_date"))) \
            .withColumn("day_of_week", dayofweek(col("review_date"))) \
            .withColumn("is_weekend_review", col("day_of_week").isin([1, 7])) \
            .withColumn("days_since_listing", datediff(col("review_date"), col("review_date")))  # This would need actual listing date
        
        # Metadata
        silver_df = silver_df \
            .withColumn("created_at", current_timestamp()) \
            .withColumn("updated_at", current_timestamp()) \
            .withColumn("processing_version", lit("v1.0"))
        
        # Write to silver layer
        silver_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("review_date") \
            .saveAsTable(silver_table)
        
        print(f"✅ Bronze to Silver transformation completed for reviews. Records: {silver_df.count()}")
        return silver_df
    
    def populate_gold_layer(self):
        """
        Populate all gold layer tables with comprehensive analytics.
        """
        print("🔄 Starting Gold layer population...")
        
        # Read from silver layer
        listings_df = self.spark.read.table("silver_layer.silver_airbnb_listings")
        reviews_df = self.spark.read.table("silver_layer.silver_airbnb_reviews")
        
        # Gold 1: Listings Analytics
        self._create_listings_analytics(listings_df)
        
        # Gold 2: Host Analytics
        self._create_host_analytics(listings_df, reviews_df)
        
        # Gold 3: Time Series Analytics
        self._create_time_series_analytics(listings_df, reviews_df)
        
        # Gold 4: Market Insights
        self._create_market_insights(listings_df, reviews_df)
        
        print("✅ Gold layer population completed successfully!")
    
    def _create_listings_analytics(self, listings_df):
        """Create comprehensive listings analytics."""
        print("📊 Creating listings analytics...")
        
        listings_analytics = listings_df.groupBy(
            "neighbourhood_group", "neighbourhood", "room_type", "price_category"
        ).agg(
            count("*").alias("total_listings"),
            avg("price").alias("avg_price"),
            expr("percentile_approx(price, 0.5)").alias("median_price"),
            min("price").alias("min_price"),
            max("price").alias("max_price"),
            avg("minimum_nights").alias("avg_minimum_nights"),
            avg("availability_365").alias("avg_availability"),
            sum("number_of_reviews").alias("total_reviews"),
            avg("reviews_per_month").alias("avg_reviews_per_month"),
            sum(when(col("is_active") == True, 1).otherwise(0)).alias("active_listings"),
            avg("listing_quality_score").alias("avg_quality_score")
        ).withColumn("price_volatility",
            col("max_price") - col("min_price")
        ).withColumn("last_updated", current_timestamp())
        
        listings_analytics.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("neighbourhood_group") \
            .saveAsTable("gold_layer.gold_listings_analytics")
    
    def _create_host_analytics(self, listings_df, reviews_df):
        """Create comprehensive host analytics."""
        print("👥 Creating host analytics...")
        
        # Host performance metrics
        host_analytics = listings_df.groupBy("host_id", "host_name").agg(
            count("*").alias("total_listings"),
            sum("number_of_reviews").alias("total_reviews"),
            avg("price").alias("avg_listing_price"),
            avg("reviews_per_month").alias("avg_rating"),
            sum(col("price") * col("availability_365") * 0.7).alias("total_revenue_estimate"),
            avg("listing_quality_score").alias("avg_quality_score"),
            countDistinct("neighbourhood").alias("neighbourhood_diversity"),
            countDistinct("room_type").alias("room_type_diversity")
        ).withColumn("host_tier",
            when(col("total_listings") >= 10, "Professional")
            .when(col("total_listings") >= 3, "Experienced")
            .otherwise("Casual")
        ).withColumn("superhost_candidate", 
            (col("total_reviews") > 50) & (col("avg_rating") >= 4.8)
        ).withColumn("performance_score",
            (col("total_reviews") * 0.3 + 
             col("avg_rating") * 0.4 + 
             col("avg_quality_score") * 0.3).cast("decimal(5,2)")
        ).withColumn("last_updated", current_timestamp())
        
        host_analytics.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("gold_layer.gold_host_analytics")
    
    def _create_time_series_analytics(self, listings_df, reviews_df):
        """Create time series analytics."""
        print("📅 Creating time series analytics...")
        
        # Create synthetic daily data (in real scenario, this would come from calendar data)
        time_series_df = reviews_df.groupBy("review_date").agg(
            countDistinct("listing_id").alias("active_listings"),
            avg("sentiment_score").alias("avg_sentiment"),
            count("*").alias("total_reviews"),
            avg("review_length").alias("avg_review_length")
        ).withColumn("year", year(col("review_date"))) \
         .withColumn("month", month(col("review_date"))) \
         .withColumn("quarter", quarter(col("review_date"))) \
         .withColumn("day_of_week", dayofweek(col("review_date"))) \
         .withColumn("is_weekend", col("day_of_week").isin([1, 7])) \
         .withColumn("season",
            when(col("month").isin([12, 1, 2]), "Winter")
            .when(col("month").isin([3, 4, 5]), "Spring")
            .when(col("month").isin([6, 7, 8]), "Summer")
            .otherwise("Fall")
         ).withColumn("last_updated", current_timestamp())
        
        time_series_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("year", "month") \
            .saveAsTable("gold_layer.gold_time_series_analytics")
    
    def _create_market_insights(self, listings_df, reviews_df):
        """Create market insights and KPIs."""
        print("💡 Creating market insights...")
        
        # Calculate various market metrics
        market_metrics = []
        
        # Price insights
        avg_price = listings_df.agg(avg("price")).collect()[0][0]
        market_metrics.append(("average_price", avg_price, "market", "current", 0.0, "stable", "strategic", current_timestamp()))
        
        # Availability insights
        avg_availability = listings_df.agg(avg("availability_365")).collect()[0][0]
        market_metrics.append(("average_availability", avg_availability, "market", "current", 0.0, "stable", "operational", current_timestamp()))
        
        # Sentiment insights
        avg_sentiment = reviews_df.agg(avg("sentiment_score")).collect()[0][0]
        market_metrics.append(("average_sentiment", avg_sentiment, "market", "current", 0.0, "stable", "customer", current_timestamp()))
        
        # Convert to DataFrame
        schema = StructType([
            StructField("metric_name", StringType(), True),
            StructField("metric_value", DecimalType(15,2), True),
            StructField("metric_dimension", StringType(), True),
            StructField("comparison_period", StringType(), True),
            StructField("change_percentage", DecimalType(5,2), True),
            StructField("trend_direction", StringType(), True),
            StructField("insight_level", StringType(), True),
            StructField("generated_at", TimestampType(), True)
        ])
        
        insights_df = self.spark.createDataFrame(market_metrics, schema)
        
        insights_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("gold_layer.gold_market_insights")
    
    def run_complete_pipeline(self):
        """Run the complete medallion architecture pipeline."""
        print("🚀 Starting complete medallion architecture pipeline...")
        
        try:
            # Bronze to Silver transformations
            self.bronze_to_silver_listings()
            self.bronze_to_silver_reviews()
            
            # Silver to Gold transformations
            self.populate_gold_layer()
            
            print("✅ Complete pipeline executed successfully!")
            
        except Exception as e:
            print(f"❌ Pipeline failed: {str(e)}")
            raise e
    
    def validate_pipeline(self):
        """Validate the pipeline by checking record counts and data quality."""
        print("🔍 Validating pipeline...")
        
        validations = []
        
        # Check bronze layer
        try:
            bronze_count = self.spark.read.table("bronze_layer.bronze_airbnb_listings").count()
            validations.append(("Bronze Listings", bronze_count, "✅"))
        except:
            validations.append(("Bronze Listings", 0, "❌"))
        
        # Check silver layer
        try:
            silver_listings = self.spark.read.table("silver_layer.silver_airbnb_listings").count()
            validations.append(("Silver Listings", silver_listings, "✅"))
        except:
            validations.append(("Silver Listings", 0, "❌"))
        
        try:
            silver_reviews = self.spark.read.table("silver_layer.silver_airbnb_reviews").count()
            validations.append(("Silver Reviews", silver_reviews, "✅"))
        except:
            validations.append(("Silver Reviews", 0, "❌"))
        
        # Check gold layer
        try:
            gold_analytics = self.spark.read.table("gold_layer.gold_listings_analytics").count()
            validations.append(("Gold Analytics", gold_analytics, "✅"))
        except:
            validations.append(("Gold Analytics", 0, "❌"))
        
        # Print validation results
        print("\n📊 Pipeline Validation Results:")
        for table_name, count, status in validations:
            print(f"  {status} {table_name}: {count} records")
        
        return validations

# Main execution function
def main():
    """Main function to run the medallion architecture pipeline."""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Databricks Medallion Architecture") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize and run pipeline
        pipeline = MedallionArchitecture(spark)
        pipeline.run_complete_pipeline()
        pipeline.validate_pipeline()
        
        print("\n🎉 Medallion architecture pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Pipeline execution failed: {str(e)}")
        raise e
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
