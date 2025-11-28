from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import pandas as pd
import mysql.connector

def run_etl():
    # Initialize Spark
    spark = SparkSession.builder.appName("Sales ETL Pipeline").getOrCreate()

    # Load raw CSV file
    df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

    # Data cleaning & transformation
    cleaned_df = df.withColumn("total_amount", col("price") * col("quantity"))

    # Aggregate total sales by customer
    final_df = cleaned_df.groupBy("customer_name").agg(_sum("total_amount").alias("total_spent"))

    # Convert Spark DataFrame to Pandas for MySQL insertion
    pandas_df = final_df.toPandas()

    # Connect to MySQL using mysql-connector-python
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="admin",
        database="etl_db"
    )
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_sales (
            customer_name VARCHAR(255),
            total_spent DOUBLE
        )
    """)

    # Insert data into MySQL
    for _, row in pandas_df.iterrows():
        cursor.execute("""
            INSERT INTO customer_sales (customer_name, total_spent)
            VALUES (%s, %s)
        """, (row['customer_name'], row['total_spent']))

    conn.commit()
    cursor.close()
    conn.close()

    print("ETL Job Completed Successfully!")

    # Stop Spark session
    spark.stop()
