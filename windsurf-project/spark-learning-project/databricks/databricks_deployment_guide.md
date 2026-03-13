# Databricks Medallion Architecture Deployment Guide

## 🎯 Overview

This guide provides step-by-step instructions for deploying the Bronze-Silver-Gold medallion architecture for Airbnb data analysis on Databricks.

## 📋 Prerequisites

### Databricks Requirements
- Databricks Workspace (Unity Catalog recommended)
- Cluster with:
  - Spark 3.4.x or later
  - Python 3.9+
  - At least 8GB memory for testing
  - Delta Lake enabled

### Permissions Required
- `CREATE CATALOG` permission (or use existing catalog)
- `CREATE DATABASE` permissions
- `CREATE TABLE` permissions
- Cluster administration access

## 🚀 Deployment Steps

### Step 1: Setup Workspace Structure

#### Option A: Using Unity Catalog (Recommended)
```sql
-- Create catalog if it doesn't exist
CREATE CATALOG IF NOT EXISTS airbnb_analytics;
USE CATALOG airbnb_analytics;
```

#### Option B: Using Hive Metastore
```sql
-- Use default catalog
USE CATALOG hive_metastore;
```

### Step 2: Execute DDL Script

1. **Create a new notebook in Databricks**
   - Name: `01_Create_Medallion_Architecture`
   - Language: SQL
   - Cluster: Select your target cluster

2. **Copy and execute the DDL script**
   - Copy the entire content from `databricks_medallion_architecture.sql`
   - Execute cell by cell or all at once
   - Verify all tables and views are created successfully

3. **Verify creation:**
```sql
-- Check databases
SHOW DATABASES;

-- Check tables in each layer
SHOW TABLES IN bronze_layer;
SHOW TABLES IN silver_layer;
SHOW TABLES IN gold_layer;
```

### Step 3: Deploy Spark Transformations

1. **Create a new notebook for transformations**
   - Name: `02_Spark_Transformations`
   - Language: Python
   - Cluster: Same cluster as DDL

2. **Install required packages** (if needed):
```python
%pip install --upgrade pandas numpy
```

3. **Copy the transformation code**
   - Copy the entire content from `spark_transformations.py`
   - Execute the pipeline initialization

4. **Run the complete pipeline:**
```python
# Initialize the pipeline
pipeline = MedallionArchitecture()

# Run the complete pipeline
pipeline.run_complete_pipeline()

# Validate the results
pipeline.validate_pipeline()
```

### Step 4: Load Sample Data

#### Option A: Using Public Airbnb Dataset
```python
# In a new notebook: 03_Load_Sample_Data
%python

from pyspark.sql.functions import *

# Load public Airbnb data (replace with actual URL)
# Example using a public dataset
listings_df = spark.read.csv(
    "/databricks-datasets/airbnb/listings.csv",
    header=True,
    inferSchema=True
)

# Transform to match bronze schema
bronze_listings = listings_df.select(
    col("id").cast("string"),
    col("name"),
    col("host_id").cast("string"),
    col("host_name"),
    col("neighbourhood_group"),
    col("neighbourhood"),
    col("latitude"),
    col("longitude"),
    col("room_type"),
    col("price").cast("string"),
    col("minimum_nights").cast("string"),
    col("number_of_reviews").cast("string"),
    col("last_review").cast("string"),
    col("reviews_per_month").cast("string"),
    col("calculated_host_listings_count").cast("string"),
    col("availability_365").cast("string"),
    lit("/databricks-datasets/airbnb/listings.csv").alias("raw_file_path"),
    current_timestamp().alias("ingestion_timestamp"),
    lit("public_dataset").alias("source_system")
)

# Write to bronze layer
bronze_listings.write.format("delta").mode("overwrite").saveAsTable("bronze_layer.bronze_airbnb_listings")
```

#### Option B: Using Sample Data from DDL
```sql
-- The DDL script already includes sample data
-- Just verify it exists:
SELECT COUNT(*) FROM bronze_layer.bronze_airbnb_listings;
```

### Step 5: Execute Stored Procedures

```sql
-- Execute the transformations using stored procedures
CALL bronze_to_silver_listings();
CALL bronze_to_silver_reviews();
CALL populate_gold_analytics();
```

### Step 6: Create Jobs for Automation

1. **Navigate to Jobs in Databricks**
2. **Create new job**: `Airbnb_Medallion_Pipeline`
3. **Add tasks:**

#### Task 1: Bronze to Silver
- **Name**: `Bronze_to_Silver_Transform`
- **Type**: Notebook
- **Notebook**: `02_Spark_Transformations`
- **Parameters**: None
- **Cluster**: Your target cluster

#### Task 2: Gold Layer Updates
- **Name**: `Gold_Analytics_Update`
- **Type**: Notebook
- **Notebook**: `02_Spark_Transformations`
- **Parameters**: None
- **Dependencies**: Task 1

#### Task 3: Validation
- **Name**: `Pipeline_Validation`
- **Type**: Notebook
- **Notebook**: `02_Spark_Transformations`
- **Parameters**: None
- **Dependencies**: Task 2

### Step 7: Set Up Monitoring

#### Create Dashboard
1. **Go to Dashboards → Create Dashboard**
2. **Name**: `Airbnb Analytics Dashboard`
3. **Add visualizations:**

```sql
-- Query 1: Listings by Neighborhood
SELECT 
    neighbourhood_group,
    COUNT(*) as total_listings,
    AVG(price) as avg_price
FROM silver_layer.silver_airbnb_listings
GROUP BY neighbourhood_group
ORDER BY total_listings DESC;

-- Query 2: Host Performance
SELECT 
    host_tier,
    COUNT(*) as host_count,
    AVG(total_revenue_estimate) as avg_revenue
FROM gold_layer.gold_host_analytics
GROUP BY host_tier;

-- Query 3: Time Series Trends
SELECT 
    review_date,
    total_reviews,
    avg_sentiment
FROM gold_layer.gold_time_series_analytics
WHERE review_date >= DATE_SUB(CURRENT_DATE, 30)
ORDER BY review_date;
```

## 🔧 Configuration Options

### Cluster Configuration
```json
{
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 2,
  "spark_conf": {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true"
  }
}
```

### Delta Lake Optimization
```sql
-- Optimize tables after loading
OPTIMIZE bronze_layer.bronze_airbnb_listings;
OPTIMIZE silver_layer.silver_airbnb_listings;
OPTIMIZE gold_layer.gold_listings_analytics;

-- Z-order for better query performance
OPTIMIZE silver_layer.silver_airbnb_listings ZORDER BY (neighbourhood_group, price);
OPTIMIZE gold_layer.gold_listings_analytics ZORDER BY (neighbourhood_group);
```

## 📊 Validation Queries

### Bronze Layer Validation
```sql
-- Check data quality in bronze layer
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT id) as unique_listings,
    COUNT(DISTINCT host_id) as unique_hosts,
    COUNT(CASE WHEN price IS NULL OR price = '' THEN 1 END) as missing_price
FROM bronze_layer.bronze_airbnb_listings;
```

### Silver Layer Validation
```sql
-- Validate transformations
SELECT 
    neighbourhood_group,
    COUNT(*) as total_listings,
    AVG(price) as avg_price,
    AVG(reviews_per_month) as avg_reviews,
    COUNT(CASE WHEN is_active = true THEN 1 END) as active_listings
FROM silver_layer.silver_airbnb_listings
GROUP BY neighbourhood_group;
```

### Gold Layer Validation
```sql
-- Validate aggregations
SELECT 
    COUNT(*) as analytics_records,
    SUM(total_listings) as total_listings_analyzed,
    AVG(avg_price) as overall_avg_price
FROM gold_layer.gold_listings_analytics;
```

## 🔄 Production Considerations

### Data Quality Monitoring
```python
# Add to your pipeline
def data_quality_checks(spark):
    """Run data quality checks on each layer."""
    
    # Bronze layer checks
    bronze_count = spark.read.table("bronze_layer.bronze_airbnb_listings").count()
    assert bronze_count > 0, "Bronze layer is empty!"
    
    # Silver layer checks
    silver_df = spark.read.table("silver_layer.silver_airbnb_listings")
    null_prices = silver_df.filter(col("price").isNull()).count()
    assert null_prices == 0, "Silver layer has null prices!"
    
    # Gold layer checks
    gold_count = spark.read.table("gold_layer.gold_listings_analytics").count()
    assert gold_count > 0, "Gold layer is empty!"
    
    print("✅ All data quality checks passed!")
```

### Error Handling
```python
# Add try-catch blocks around critical operations
try:
    pipeline.bronze_to_silver_listings()
except Exception as e:
    # Log error and send notification
    print(f"❌ Transformation failed: {str(e)}")
    # Send to monitoring system
    raise e
```

### Performance Optimization
```sql
-- Set up automatic optimization
ALTER TABLE bronze_layer.bronze_airbnb_listings 
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Set up Z-ordering for frequently queried columns
OPTIMIZE silver_layer.silver_airbnb_listings 
ZORDER BY (neighbourhood_group, price);
```

## 🚨 Troubleshooting

### Common Issues

1. **Permission Errors**
   ```sql
   -- Check permissions
   SHOW GRANTS ON CATALOG airbnb_analytics;
   ```

2. **Memory Issues**
   - Increase cluster memory
   - Add more workers
   - Enable adaptive query execution

3. **Delta Lake Issues**
   ```sql
   -- Check Delta version
   DESCRIBE HISTORY bronze_layer.bronze_airbnb_listings;
   ```

4. **Performance Issues**
   - Check query plans with `EXPLAIN`
   - Ensure proper partitioning
   - Use Z-ordering

### Monitoring Queries
```sql
-- Monitor table sizes
SELECT 
    table_name,
    size_in_bytes,
    size_in_bytes / (1024*1024*1024) as size_gb
FROM table_sizes;

-- Monitor query performance
SELECT 
    statement_id,
    execution_time_ms,
    rows_produced
FROM query_history
ORDER BY execution_time_ms DESC
LIMIT 10;
```

## 📚 Next Steps

1. **Add more data sources** (external APIs, streaming data)
2. **Implement ML models** for price prediction
3. **Create real-time dashboards** with streaming analytics
4. **Add data governance** with Unity Catalog
5. **Implement CI/CD** for pipeline deployments

## 🎉 Success Criteria

Your deployment is successful when:
- ✅ All tables and views are created
- ✅ Sample data loads successfully
- ✅ Bronze → Silver → Gold transformations work
- ✅ Stored procedures execute without errors
- ✅ Dashboard shows meaningful visualizations
- ✅ Jobs run on schedule without failures

---

**🚀 You're now ready to run your Airbnb medallion architecture in Databricks!**
