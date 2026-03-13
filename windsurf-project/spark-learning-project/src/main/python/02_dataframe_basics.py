#!/usr/bin/env python3
"""
Apache Spark DataFrame Basics
This script demonstrates fundamental DataFrame operations in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, lit, when, count, sum as spark_sum, avg, max as spark_max, min as spark_min
import sys
from datetime import datetime

def create_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName("DataFrame Basics") \
        .master("local[*]") \
        .getOrCreate()

def create_dataframe_from_list(spark):
    """Create DataFrame from a list of tuples."""
    print("=== Creating DataFrame from List ===")
    
    data = [
        (1, "Alice", 25, "Engineering", 75000.0),
        (2, "Bob", 30, "Marketing", 65000.0),
        (3, "Charlie", 35, "Engineering", 85000.0),
        (4, "Diana", 28, "Sales", 60000.0),
        (5, "Eve", 32, "Engineering", 90000.0)
    ]
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("salary", DoubleType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    df.show()
    df.printSchema()
    
    return df

def dataframe_selection_and_filtering(df):
    """Demonstrate DataFrame selection and filtering operations."""
    print("\n=== DataFrame Selection and Filtering ===")
    
    # Select specific columns
    name_age_df = df.select("name", "age")
    print("Name and Age:")
    name_age_df.show()
    
    # Filter operations
    engineers_df = df.filter(col("department") == "Engineering")
    print("Engineers:")
    engineers_df.show()
    
    # Multiple conditions
    high_earners_df = df.filter((col("salary") > 70000) & (col("age") < 35))
    print("High earners under 35:")
    high_earners_df.show()
    
    # Using where (alias for filter)
    sales_df = df.where(col("department") == "Sales")
    print("Sales department:")
    sales_df.show()

def dataframe_aggregations(df):
    """Demonstrate DataFrame aggregation operations."""
    print("\n=== DataFrame Aggregations ===")
    
    # Group by and aggregate
    dept_stats = df.groupBy("department") \
                  .agg(
                      count("*").alias("employee_count"),
                      avg("salary").alias("avg_salary"),
                      spark_max("salary").alias("max_salary"),
                      spark_min("age").alias("min_age")
                  )
    print("Department Statistics:")
    dept_stats.show()
    
    # Overall statistics
    print("Overall Statistics:")
    df.select(
        count("*").alias("total_employees"),
        avg("age").alias("avg_age"),
        avg("salary").alias("avg_salary")
    ).show()

def dataframe_transformations(df):
    """Demonstrate DataFrame transformation operations."""
    print("\n=== DataFrame Transformations ===")
    
    # Add new columns
    df_with_bonus = df.withColumn("bonus", col("salary") * 0.1)
    print("With Bonus:")
    df_with_bonus.show()
    
    # Conditional column
    df_with_category = df.withColumn(
        "age_category",
        when(col("age") < 30, "Young")
        .when(col("age") < 35, "Mid-age")
        .otherwise("Senior")
    )
    print("With Age Category:")
    df_with_category.show()
    
    # Rename columns
    renamed_df = df.withColumnRenamed("salary", "annual_salary")
    print("Renamed Columns:")
    renamed_df.show()
    
    # Sort operations
    sorted_df = df.orderBy(col("salary").desc())
    print("Sorted by Salary (desc):")
    sorted_df.show()

def dataframe_joins(spark):
    """Demonstrate DataFrame join operations."""
    print("\n=== DataFrame Joins ===")
    
    # Create employee DataFrame
    employees = [
        (1, "Alice", 1),
        (2, "Bob", 2),
        (3, "Charlie", 1),
        (4, "Diana", 3),
        (5, "Eve", 1)
    ]
    emp_schema = StructType([
        StructField("emp_id", IntegerType(), True),
        StructField("emp_name", StringType(), True),
        StructField("dept_id", IntegerType(), True)
    ])
    emp_df = spark.createDataFrame(employees, emp_schema)
    
    # Create department DataFrame
    departments = [
        (1, "Engineering", "Tech"),
        (2, "Marketing", "Business"),
        (3, "Sales", "Business"),
        (4, "HR", "Admin")
    ]
    dept_schema = StructType([
        StructField("dept_id", IntegerType(), True),
        StructField("dept_name", StringType(), True),
        StructField("division", StringType(), True)
    ])
    dept_df = spark.createDataFrame(departments, dept_schema)
    
    # Inner join
    inner_join = emp_df.join(dept_df, "dept_id", "inner")
    print("Inner Join:")
    inner_join.show()
    
    # Left join
    left_join = emp_df.join(dept_df, "dept_id", "left")
    print("Left Join:")
    left_join.show()
    
    # Right join
    right_join = emp_df.join(dept_df, "dept_id", "right")
    print("Right Join:")
    right_join.show()

def dataframe_file_operations(spark, df):
    """Demonstrate reading and writing DataFrames to files."""
    print("\n=== DataFrame File Operations ===")
    
    # Write to CSV
    output_path = "/tmp/employees.csv"
    df.write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"DataFrame written to {output_path}")
    
    # Read from CSV
    read_df = spark.read.option("header", "true").csv(output_path)
    print("Read from CSV:")
    read_df.show()
    
    # Write to JSON
    json_path = "/tmp/employees.json"
    df.write.mode("overwrite").json(json_path)
    print(f"DataFrame written to {json_path}")
    
    # Read from JSON
    json_df = spark.read.json(json_path)
    print("Read from JSON:")
    json_df.show()

def main():
    """Main function to run DataFrame examples."""
    spark = create_spark_session()
    
    try:
        # Run examples
        df = create_dataframe_from_list(spark)
        dataframe_selection_and_filtering(df)
        dataframe_aggregations(df)
        dataframe_transformations(df)
        dataframe_joins(spark)
        dataframe_file_operations(spark, df)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
