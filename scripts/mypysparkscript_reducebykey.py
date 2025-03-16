from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

try:
    # Initialize Spark session
    spark = SparkSession.builder.appName("ReduceByKeyTransformation").getOrCreate()

    # Load the CSV file using Spark
    file_path = 'gs://spark-job-data-bucket/data/year-2025/month-01/Financial.csv'
    df_spark = spark.read.option("header", "true").csv(file_path)

    # Clean financial columns: Remove "$", "B", and ",", then convert to float
    financial_columns = ["Sales", "Profit", "Assets", "Market Value"]
    for col_name in financial_columns:
        df_spark = df_spark.withColumn(
            col_name, 
            regexp_replace(col(col_name), "[$B,]", "").cast("double")
        )

    # Convert DataFrame to RDD
    rdd = df_spark.rdd.map(lambda row: (
        row["Country"],
        (row["Sales"] or 0, row["Profit"] or 0, row["Assets"] or 0, row["Market Value"] or 0)
    ))

    # Perform reduceByKey to sum up financial values for each country
    rdd_reduced = rdd.reduceByKey(
        lambda x, y: (
            x[0] + y[0],  # Sum Sales
            x[1] + y[1],  # Sum Profit
            x[2] + y[2],  # Sum Assets
            x[3] + y[3]   # Sum Market Value
        )
    )

    # Convert the reduced RDD back to a DataFrame
    columns = ["Country", "Sales", "Profit", "Assets", "Market Value"]
    df_reduced = rdd_reduced.map(lambda x: (x[0], *x[1])).toDF(columns)

    # Show the transformed dataset
    df_reduced.show()

    # Write the resulting DataFrame to Parquet in overwrite mode
    df_reduced.write.option("header", "true").mode("overwrite").parquet("gs://spark-job-data-bucket/processed-data/reducebykey-data/")

except Exception as e:
    # Handle any exceptions or errors
    print("Error occurred:", str(e))

finally:
    # Stop SparkSession
    spark.stop()
