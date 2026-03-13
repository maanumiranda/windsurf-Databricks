# Apache Spark Learning Project

A comprehensive project for learning and practicing Apache Spark with both Python (PySpark) and Scala examples.

## 📁 Project Structure

```
spark-learning-project/
├── build.sbt                    # Scala build configuration
├── requirements.txt             # Python dependencies
├── README.md                    # This file
├── src/
│   ├── main/
│   │   ├── python/              # PySpark examples
│   │   │   ├── 01_rdd_basics.py
│   │   │   ├── 02_dataframe_basics.py
│   │   │   ├── 03_spark_sql.py
│   │   │   └── 04_streaming_basics.py
│   │   └── scala/               # Scala Spark examples
│   │       └── RDDBasics.scala
│   └── test/                    # Test files (you can add your own)
├── data/
│   ├── input/                   # Sample datasets for practice
│   │   ├── employees.csv
│   │   ├── sales_data.csv
│   │   └── products.jsonl
│   └── output/                  # Output directory for processed data
├── docs/                        # Documentation (you can add your own)
└── configs/                     # Configuration files
```

## 🚀 Getting Started

### Prerequisites

- Java 8 or higher (for Scala examples)
- Python 3.7 or higher (for PySpark examples)
- Apache Spark 3.4.1 (or compatible version)
- SBT (for Scala examples)

### Installation

#### For PySpark (Python)

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set SPARK_HOME environment variable:**
   ```bash
   export SPARK_HOME=/path/to/spark
   export PATH=$PATH:$SPARK_HOME/bin
   ```

3. **Run PySpark examples:**
   ```bash
   python src/main/python/01_rdd_basics.py
   python src/main/python/02_dataframe_basics.py
   python src/main/python/03_spark_sql.py
   python src/main/python/04_streaming_basics.py
   ```

#### For Scala

1. **Install SBT** (if not already installed):
   - macOS: `brew install sbt`
   - Ubuntu: `sudo apt-get install sbt`
   - Windows: Download from https://www.scala-sbt.org/

2. **Compile and run Scala examples:**
   ```bash
   sbt compile
   sbt "runMain RDDBasics"
   ```

## 📚 Learning Path

### 1. RDD Basics (`01_rdd_basics.py` / `RDDBasics.scala`)
**Topics covered:**
- RDD creation from collections and files
- Transformations: map, filter, flatMap, union, intersection
- Actions: collect, count, reduce, first, take
- Word count example|
- Caching and persistence

**Learning objectives:**
- Understand Spark's core abstraction (RDD)
- Master lazy evaluation concepts
- Learn transformation vs action operations

### 2. DataFrame Basics (`02_dataframe_basics.py`)
**Topics covered:**
- DataFrame creation from various sources
- Schema definition and inference
- Selection and filtering operations
- Aggregations and group-by operations
- Joins between DataFrames
- File I/O operations (CSV, JSON)

**Learning objectives:**
- Understand DataFrame API advantages over RDDs
- Master Catalyst optimizer concepts
- Learn structured data processing

### 3. Spark SQL (`03_spark_sql.py`)
**Topics covered:**
- Creating temporary views
- Basic SQL queries (SELECT, WHERE, ORDER BY)
- Aggregate functions and GROUP BY
- JOIN operations (INNER, LEFT, RIGHT)
- Window functions (ROW_NUMBER, LAG, LEAD)
- Common Table Expressions (CTEs)
- Date and time functions

**Learning objectives:**
- Master SQL interface for Spark
- Understand query optimization
- Learn complex analytical queries

### 4. Streaming Basics (`04_streaming_basics.py`)
**Topics covered:**
- Structured Streaming fundamentals
- Reading from file sources
- Windowed aggregations
- Watermarks for handling late data
- foreachBatch for custom processing
- Memory sink for querying streams

**Learning objectives:**
- Understand streaming concepts and architecture
- Master real-time data processing
- Learn state management in streams

## 📊 Sample Datasets

### Employees Dataset (`data/input/employees.csv`)
Contains employee information including:
- Personal details (name, age)
- Department and position
- Salary and hire date
- Perfect for practicing aggregations and joins

### Sales Data (`data/input/sales_data.csv`)
Sales transaction data including:
- Order details and customer information
- Product information and pricing
- Regional data for geographical analysis
- Ideal for time-series and aggregation exercises

### Products Dataset (`data/input/products.jsonl`)
Product catalog in JSON Lines format:
- Product details and specifications
- Category and brand information
- Stock levels and pricing
- Great for JSON processing practice

## 🛠️ Practice Exercises

### Beginner Level
1. **RDD Operations:**
   - Calculate average salary by department using RDDs
   - Find top 5 most expensive products using RDD transformations
   - Implement a custom partitioner for employee data

2. **DataFrame Operations:**
   - Create a DataFrame from the products JSONL file
   - Calculate monthly sales totals from sales data
   - Find employees earning above department average

### Intermediate Level
1. **Spark SQL:**
   - Write a query to find year-over-year sales growth
   - Implement a rolling average of sales over 7 days
   - Create a ranked report of products by revenue

2. **Advanced Joins:**
   - Join employees with sales data to find top performers
   - Implement a self-join to find product pairs frequently bought together
   - Handle data quality issues in joins

### Advanced Level
1. **Streaming:**
   - Set up a real-time sales dashboard
   - Implement anomaly detection in streaming data
   - Create a session window for user activity analysis

2. **Performance Optimization:**
   - Compare performance of RDD vs DataFrame operations
   - Implement proper caching strategies
   - Optimize shuffle operations

## 🔧 Configuration

### Spark Configuration Examples

**For local development:**
```python
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()
```

**For production:**
```python
spark = SparkSession.builder \
    .appName("ProductionApp") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
```

## 📖 Additional Resources

### Official Documentation
- [Apache Spark Official Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Spark Programming Guide](https://spark.apache.org/docs/latest/programming-guide.html)

### Recommended Books
- "Learning Spark" by O'Reilly
- "High Performance Spark" by O'Reilly
- "Spark: The Definitive Guide" by O'Reilly

### Online Courses
- DataBricks Academy (Free Spark courses)
- Coursera: "Big Data Specialization"
- edX: "Introduction to Big Data with Apache Spark"

## 🤝 Contributing

Feel free to:
- Add new examples and exercises
- Improve existing code
- Fix bugs and issues
- Add documentation

## 📝 Tips for Learning

1. **Start with the basics**: Master RDDs before moving to DataFrames
2. **Practice with real data**: Use the provided datasets or your own
3. **Understand the concepts**: Don't just copy code, understand why it works
4. **Experiment**: Try different approaches and compare performance
5. **Read the documentation**: Official docs are your best friend

## 🐛 Common Issues and Solutions

### Memory Issues
- Increase executor memory: `--executor-memory 4g`
- Use appropriate partitioning
- Cache intermediate results wisely

### Performance Issues
- Use DataFrames instead of RDDs when possible
- Enable adaptive query execution
- Avoid UDFs when built-in functions suffice

### Serialization Issues
- Use Kryo serializer for better performance
- Register custom classes for Kryo

## 📞 Support

If you encounter issues:
1. Check Spark logs for error messages
2. Verify your Spark installation
3. Ensure data files are in correct format
4. Check memory and resource allocation

Happy learning! 🚀
