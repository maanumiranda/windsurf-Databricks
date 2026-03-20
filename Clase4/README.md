# Spark Data Processing & Analytics Project

## Project Overview

This comprehensive project demonstrates Apache Spark data processing and manipulation techniques using Netflix dataset. The project covers:

- **Spark SQL Data Analysis**: Using DataFrames as temporary views with ANSI SQL operations
- **Structured Data Manipulation**: Column operations, filtering, aggregations, joins, sorting, and deduplication
- **Transformations vs Actions**: Understanding Spark's lazy evaluation model
- **Mini Project**: Business questions answered using both SQL and DataFrame APIs

## Dataset

The project uses a Netflix dataset containing information about movies and TV shows, including:
- Show metadata (title, type, director, cast, etc.)
- Content ratings and duration
- Release dates and countries
- Genre classifications

## Project Structure

```
├── README.md                 # Project overview and documentation
├── data/
│   ├── netflix_ddl.sql       # DDL statements for Databricks
│   └── netflix_data.csv      # Sample Netflix dataset
├── notebooks/
│   ├── 01_spark_sql_analysis.py      # Spark SQL examples
│   ├── 02_data_manipulation.py      # DataFrame operations
│   ├── 03_transformations_actions.py # Transformations vs Actions
│   └── 04_mini_project.py            # Business questions project
└── requirements.txt          # Python dependencies
```

## Learning Objectives

After completing this project, you will understand:

1. **Spark DataFrame API**: How to create, manipulate, and query DataFrames
2. **Spark SQL**: How to use standard SQL with DataFrames
3. **Data Processing Patterns**: Common operations for data cleaning and analysis
4. **Performance Considerations**: Understanding lazy evaluation and optimization
5. **Practical Applications**: Real-world data analysis scenarios

## Prerequisites

- Apache Spark (or Databricks environment)
- Python 3.7+
- PySpark library
- Basic understanding of SQL and data concepts

## Getting Started

1. Set up your Spark environment (local or Databricks)
2. Run the DDL script to create the Netflix table
3. Execute the notebooks in order for progressive learning
4. Complete the mini project to test your skills

## Business Questions (Mini Project)

The mini project challenges you to answer questions like:
- What are the most popular genres on Netflix?
- Which countries produce the most content?
- How has content availability changed over time?
- What is the average duration by content type?

Both SQL and DataFrame API approaches are demonstrated, showing how different methods can achieve the same results.
