#!/usr/bin/env python3
"""
Apache Spark SQL Examples
This script demonstrates Spark SQL operations and capabilities.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import col, lit, when, count, sum as spark_sum, avg, max as spark_max, min as spark_min, date_add, current_date, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead
import sys
from datetime import datetime, date

def create_spark_session():
    """Create and return a Spark session with Hive support."""
    return SparkSession.builder \
        .appName("Spark SQL Examples") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

def create_employees_table(spark):
    """Create employees DataFrame and register as a temporary view."""
    print("=== Creating Employees Table ===")
    
    data = [
        (1, "Alice", "Johnson", 25, "Engineering", 75000.0, date(2022, 1, 15), "Manager"),
        (2, "Bob", "Smith", 30, "Marketing", 65000.0, date(2021, 6, 1), "Analyst"),
        (3, "Charlie", "Brown", 35, "Engineering", 85000.0, date(2020, 3, 10), "Senior Developer"),
        (4, "Diana", "Prince", 28, "Sales", 60000.0, date(2022, 8, 20), "Sales Rep"),
        (5, "Eve", "Wilson", 32, "Engineering", 90000.0, date(2019, 11, 5), "Tech Lead"),
        (6, "Frank", "Miller", 29, "HR", 55000.0, date(2022, 2, 14), "HR Specialist"),
        (7, "Grace", "Davis", 26, "Marketing", 62000.0, date(2022, 9, 1), "Marketing Coordinator"),
        (8, "Henry", "Garcia", 38, "Engineering", 95000.0, date(2018, 7, 12), "Architect"),
        (9, "Iris", "Martinez", 31, "Sales", 68000.0, date(2021, 4, 8), "Sales Manager"),
        (10, "Jack", "Anderson", 27, "Engineering", 72000.0, date(2022, 5, 20), "Developer")
    ]
    
    schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("hire_date", DateType(), True),
        StructField("position", StringType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("employees")
    
    print("Employees table:")
    spark.sql("SELECT * FROM employees ORDER BY employee_id").show()
    
    return df

def create_departments_table(spark):
    """Create departments DataFrame and register as a temporary view."""
    print("\n=== Creating Departments Table ===")
    
    data = [
        (1, "Engineering", "Tech", "Building A", 50),
        (2, "Marketing", "Business", "Building B", 15),
        (3, "Sales", "Business", "Building B", 20),
        (4, "HR", "Admin", "Building A", 8),
        (5, "Finance", "Business", "Building C", 12)
    ]
    
    schema = StructType([
        StructField("dept_id", IntegerType(), True),
        StructField("dept_name", StringType(), True),
        StructField("division", StringType(), True),
        StructField("location", StringType(), True),
        StructField("budget_allocated", IntegerType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("departments")
    
    print("Departments table:")
    spark.sql("SELECT * FROM departments ORDER BY dept_id").show()
    
    return df

def basic_sql_queries(spark):
    """Demonstrate basic SQL queries."""
    print("\n=== Basic SQL Queries ===")
    
    # Simple SELECT with WHERE
    print("Employees in Engineering department:")
    spark.sql("""
        SELECT first_name, last_name, age, salary 
        FROM employees 
        WHERE department = 'Engineering'
    """).show()
    
    # ORDER BY and LIMIT
    print("Top 3 highest paid employees:")
    spark.sql("""
        SELECT first_name, last_name, department, salary
        FROM employees
        ORDER BY salary DESC
        LIMIT 3
    """).show()
    
    # LIKE operator
    print("Employees with names starting with 'A':")
    spark.sql("""
        SELECT first_name, last_name
        FROM employees
        WHERE first_name LIKE 'A%'
    """).show()
    
    # IN operator
    print("Employees in Engineering or Sales:")
    spark.sql("""
        SELECT first_name, last_name, department
        FROM employees
        WHERE department IN ('Engineering', 'Sales')
    """).show()

def aggregate_sql_queries(spark):
    """Demonstrate aggregate SQL queries."""
    print("\n=== Aggregate SQL Queries ===")
    
    # GROUP BY with multiple aggregates
    print("Department statistics:")
    spark.sql("""
        SELECT 
            department,
            COUNT(*) as employee_count,
            AVG(salary) as avg_salary,
            MAX(salary) as max_salary,
            MIN(salary) as min_salary,
            SUM(salary) as total_salary
        FROM employees
        GROUP BY department
        ORDER BY avg_salary DESC
    """).show()
    
    # HAVING clause
    print("Departments with more than 2 employees:")
    spark.sql("""
        SELECT 
            department,
            COUNT(*) as employee_count,
            AVG(salary) as avg_salary
        FROM employees
        GROUP BY department
        HAVING COUNT(*) > 2
    """).show()
    
    # CASE statement
    print("Employees with salary categories:")
    spark.sql("""
        SELECT 
            first_name,
            last_name,
            salary,
            CASE 
                WHEN salary < 60000 THEN 'Low'
                WHEN salary < 80000 THEN 'Medium'
                ELSE 'High'
            END as salary_category
        FROM employees
        ORDER BY salary DESC
    """).show()

def join_sql_queries(spark):
    """Demonstrate SQL JOIN operations."""
    print("\n=== SQL JOIN Queries ===")
    
    # INNER JOIN
    print("Employees with their departments:")
    spark.sql("""
        SELECT 
            e.first_name,
            e.last_name,
            e.department,
            d.division,
            d.location
        FROM employees e
        INNER JOIN departments d ON e.department = d.dept_name
    """).show()
    
    # LEFT JOIN
    print("All departments with employee counts (including those with no employees):")
    spark.sql("""
        SELECT 
            d.dept_name,
            d.division,
            COUNT(e.employee_id) as employee_count
        FROM departments d
        LEFT JOIN employees e ON d.dept_name = e.department
        GROUP BY d.dept_name, d.division
        ORDER BY employee_count DESC
    """).show()
    
    # Self JOIN
    print("Employees earning more than the average salary in their department:")
    spark.sql("""
        SELECT 
            e1.first_name,
            e1.last_name,
            e1.department,
            e1.salary,
            dept_avg.avg_dept_salary
        FROM employees e1
        INNER JOIN (
            SELECT 
                department,
                AVG(salary) as avg_dept_salary
            FROM employees
            GROUP BY department
        ) dept_avg ON e1.department = dept_avg.department
        WHERE e1.salary > dept_avg.avg_dept_salary
    """).show()

def window_functions_sql(spark):
    """Demonstrate SQL window functions."""
    print("\n=== SQL Window Functions ===")
    
    # ROW_NUMBER
    print("Employees ranked by salary within department:")
    spark.sql("""
        SELECT 
            first_name,
            last_name,
            department,
            salary,
            ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank_in_dept
        FROM employees
        ORDER BY department, rank_in_dept
    """).show()
    
    # LAG and LEAD
    print("Employee salary comparison with previous/next in same department:")
    spark.sql("""
        SELECT 
            first_name,
            last_name,
            department,
            salary,
            LAG(salary, 1, 0) OVER (PARTITION BY department ORDER BY salary) as prev_salary,
            LEAD(salary, 1, 0) OVER (PARTITION BY department ORDER BY salary) as next_salary
        FROM employees
        ORDER BY department, salary
    """).show()
    
    # Running total
    print("Running total of salaries:")
    spark.sql("""
        SELECT 
            first_name,
            last_name,
            salary,
            SUM(salary) OVER (ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
        FROM employees
        ORDER BY salary DESC
    """).show()

def date_time_sql(spark):
    """Demonstrate date and time SQL operations."""
    print("\n=== Date and Time SQL Operations ===")
    
    # Date functions
    print("Employee tenure information:")
    spark.sql("""
        SELECT 
            first_name,
            last_name,
            hire_date,
            DATEDIFF(CURRENT_DATE(), hire_date) as days_employed,
            DATE_ADD(hire_date, 365) as first_anniversary,
            YEAR(hire_date) as hire_year,
            MONTH(hire_date) as hire_month,
            DATE_FORMAT(hire_date, 'MMMM yyyy') as formatted_hire_date
        FROM employees
        ORDER BY hire_date
    """).show()

def cte_and_subqueries(spark):
    """Demonstrate Common Table Expressions and subqueries."""
    print("\n=== CTE and Subqueries ===")
    
    # CTE example
    print("Using CTE to find departments with above-average salaries:")
    spark.sql("""
        WITH dept_avg AS (
            SELECT 
                department,
                AVG(salary) as avg_dept_salary
            FROM employees
            GROUP BY department
        ),
        high_earners AS (
            SELECT 
                e.first_name,
                e.last_name,
                e.department,
                e.salary,
                d.avg_dept_salary
            FROM employees e
            JOIN dept_avg d ON e.department = d.department
            WHERE e.salary > d.avg_dept_salary
        )
        SELECT * FROM high_earners
        ORDER BY department, salary DESC
    """).show()
    
    # Subquery example
    print("Using subquery to find highest paid employee in each department:")
    spark.sql("""
        SELECT 
            first_name,
            last_name,
            department,
            salary
        FROM employees e
        WHERE salary = (
            SELECT MAX(salary) 
            FROM employees 
            WHERE department = e.department
        )
        ORDER BY salary DESC
    """).show()

def main():
    """Main function to run Spark SQL examples."""
    spark = create_spark_session()
    
    try:
        # Create tables
        employees_df = create_employees_table(spark)
        departments_df = create_departments_table(spark)
        
        # Run SQL examples
        basic_sql_queries(spark)
        aggregate_sql_queries(spark)
        join_sql_queries(spark)
        window_functions_sql(spark)
        date_time_sql(spark)
        cte_and_subqueries(spark)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
