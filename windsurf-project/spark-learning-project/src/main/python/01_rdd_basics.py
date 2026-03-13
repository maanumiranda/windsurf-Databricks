#!/usr/bin/env python3
"""
Apache Spark RDD Basics
This script demonstrates fundamental RDD operations in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext
import sys

def create_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName("RDD Basics") \
        .master("local[*]") \
        .getOrCreate()

def rdd_creation_examples(sc):
    """Demonstrate different ways to create RDDs."""
    print("=== RDD Creation Examples ===")
    
    # Create RDD from a list
    numbers_rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    print(f"Numbers RDD: {numbers_rdd.collect()}")
    
    # Create RDD from range
    range_rdd = sc.parallelize(range(1, 11))
    print(f"Range RDD: {range_rdd.collect()}")
    
    # Create RDD from text file (will create a sample file first)
    sample_text = ["Hello Spark", "RDD operations", "Learning Apache Spark"]
    with open("/tmp/sample.txt", "w") as f:
        for line in sample_text:
            f.write(line + "\n")
    
    text_rdd = sc.textFile("/tmp/sample.txt")
    print(f"Text RDD: {text_rdd.collect()}")
    
    return numbers_rdd, range_rdd, text_rdd

def rdd_transformations(numbers_rdd):
    """Demonstrate RDD transformations."""
    print("\n=== RDD Transformations ===")
    
    # Map transformation
    squared_rdd = numbers_rdd.map(lambda x: x ** 2)
    print(f"Squared numbers: {squared_rdd.collect()}")
    
    # Filter transformation
    even_rdd = numbers_rdd.filter(lambda x: x % 2 == 0)
    print(f"Even numbers: {even_rdd.collect()}")
    
    # FlatMap transformation
    words_rdd = numbers_rdd.flatMap(lambda x: [x, x * 2])
    print(f"FlatMap result: {words_rdd.collect()}")
    
    # Union transformation
    more_numbers = sc.parallelize([11, 12, 13])
    union_rdd = numbers_rdd.union(more_numbers)
    print(f"Union result: {union_rdd.collect()}")
    
    return squared_rdd, even_rdd, words_rdd, union_rdd

def rdd_actions(numbers_rdd):
    """Demonstrate RDD actions."""
    print("\n=== RDD Actions ===")
    
    # Collect action
    collected = numbers_rdd.collect()
    print(f"Collected: {collected}")
    
    # Count action
    count = numbers_rdd.count()
    print(f"Count: {count}")
    
    # Reduce action
    sum_result = numbers_rdd.reduce(lambda a, b: a + b)
    print(f"Sum: {sum_result}")
    
    # First action
    first = numbers_rdd.first()
    print(f"First element: {first}")
    
    # Take action
    first_three = numbers_rdd.take(3)
    print(f"First three elements: {first_three}")
    
    # Count by value
    value_counts = numbers_rdd.map(lambda x: (x, 1)).countByKey()
    print(f"Value counts: {dict(value_counts)}")

def word_count_example(sc):
    """Classic word count example."""
    print("\n=== Word Count Example ===")
    
    # Sample text
    text = ["hello world", "hello spark", "spark is great", "hello again"]
    text_rdd = sc.parallelize(text)
    
    # Word count logic
    counts = text_rdd.flatMap(lambda line: line.split(" ")) \
                     .map(lambda word: (word.lower(), 1)) \
                     .reduceByKey(lambda a, b: a + b) \
                     .sortBy(lambda x: x[1], ascending=False)
    
    print("Word counts:")
    for word, count in counts.collect():
        print(f"{word}: {count}")

def main():
    """Main function to run RDD examples."""
    spark = create_spark_session()
    sc = spark.sparkContext
    
    try:
        # Run examples
        numbers_rdd, range_rdd, text_rdd = rdd_creation_examples(sc)
        rdd_transformations(numbers_rdd)
        rdd_actions(numbers_rdd)
        word_count_example(sc)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
