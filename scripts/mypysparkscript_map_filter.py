from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

try:
    # Initialize Spark session
    spark = SparkSession.builder.appName("MapAndFilterExample").getOrCreate()

    # Load CSV dataset into a DataFrame from GCS
    file_path = 'gs://spark-job-data-bucket/data/year-2025/month-01/Financial.csv'  # Update the GCS path
    df = spark.read.option("header", "true").csv(file_path)

    # Clean the sales and profit columns by removing non-numeric characters
    df = df.withColumn("sales", regexp_replace(col("sales"), "[$B]", "").cast("float"))
    df = df.withColumn("profit", regexp_replace(col("profit"), "[$B]", "").cast("float"))

    # Drop rows where sales or profit are null
    df = df.na.drop(subset=["sales", "profit"])

    # Remove the decimal part by casting to integer
    df = df.withColumn("sales", col("sales").cast("int"))
    df = df.withColumn("profit", col("profit").cast("int"))

    # Convert the DataFrame to an RDD for map and filter transformations
    rdd = df.rdd

    # Use map transformation to add a "high profit" column
    mapped_rdd = rdd.map(lambda row: (
        row["Name"],
        row["Country"],
        row["sales"],
        row["profit"],
        row["Assets"],
        row["Market Value"],
        row["profit"] > 20  # Add a boolean column for high profit
    ))

    # Use filter to retain rows where sales > 100 and profit > 20
    filtered_rdd = mapped_rdd.filter(lambda row: row[2] > 100 and row[3] > 20)  # Check sales and profit

    # Convert the RDD back to a DataFrame
    new_df = filtered_rdd.toDF(["Name", "Country", "sales", "profit", "Assets", "Market Value", "High Profit"])

    # Show the resulting DataFrame
    new_df.show()

    # Perform actions (like collect, first, take)
    # Collect all the results
    collected_results = new_df.collect()
    print("Collected Results:")
    for row in collected_results:
        print(row)

    # Get the first row
    first_row = new_df.first()
    print(f"First Row: {first_row}")

    # Get the first 5 rows
    first_5_rows = new_df.take(5)
    print("First 5 Rows:")
    for row in first_5_rows:
        print(row)

    # Perform an action (like counting the filtered rows)
    row_count = new_df.count()
    print(f"Number of rows after filtering: {row_count}")

    # Write the resulting DataFrame to Parquet in overwrite mode (Save to GCS)
    output_path = "gs://spark-job-data-bucket/processed-data/map-filtered-data/"
    new_df.write.option("header", "true").mode("overwrite").parquet(output_path)

except Exception as e:
    # Handle any exceptions or errors
    print("Error occurred:", str(e))

finally:
    # Stop SparkSession
    spark.stop()
