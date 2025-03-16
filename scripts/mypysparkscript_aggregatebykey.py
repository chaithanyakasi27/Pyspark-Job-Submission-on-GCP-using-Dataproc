from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

try:
    # Initialize Spark session
    spark = SparkSession.builder.appName("FixAggregateByKey").getOrCreate()

    # Load CSV file from S3
    file_path = 'gs://spark-job-data-bucket/data/year-2025/month-01/Financial.csv'
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

    # Inspect schema to identify incorrect data types
    df.printSchema()

    # Clean and cast columns
    df_cleaned = (
        df
        .withColumn("Sales", regexp_replace(col("Sales"), "[^0-9.]", "").cast("float"))  # Remove non-numeric characters and cast to float
        .withColumn("Profit", regexp_replace(col("Profit"), "[^0-9.]", "").cast("float"))
        .withColumn("Assets", regexp_replace(col("Assets"), "[^0-9.]", "").cast("float"))
        .withColumn("Market Value", regexp_replace(col("Market Value"), "[^0-9.]", "").cast("float"))
    )

    # Verify schema after cleaning
    df_cleaned.printSchema()

    # Convert DataFrame to RDD for aggregateByKey
    rdd = df_cleaned.rdd.map(lambda row: (row["Country"], (row["Sales"], row["Profit"])))

    # Define aggregateByKey logic
    initial_value = (0.0, 0.0)  # Initial value for (sales, profit)
    seq_op = lambda acc, val: (acc[0] + (val[0] or 0), acc[1] + (val[1] or 0))  # Aggregate within partitions
    comb_op = lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # Aggregate between partitions

    # Perform aggregateByKey
    result_rdd = rdd.aggregateByKey(initial_value, seq_op, comb_op)

    # Convert result back to DataFrame
    result_df = result_rdd.map(lambda x: (x[0], x[1][0], x[1][1])).toDF(["Country", "Total Sales", "Total Profit"])

    # Show the results
    result_df.show()

    # Write the result DataFrame to Parquet with header option and overwrite mode
    result_df.write.option("header", "true").mode("overwrite").parquet("gs://spark-job-data-bucket/processed-data/aggregateByKey-data/")
    
except Exception as e:
    # Handle any exceptions or errors
    print("Error occurred:", str(e))

finally:
    # Stop SparkSession
    spark.stop()
