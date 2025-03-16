from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, sum

try:
    # Initialize Spark session
    spark = SparkSession.builder.appName("WideTransformation").getOrCreate()

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

    # Group by 'Country', sum financial values, and sort alphabetically
    df_grouped = df_spark.groupBy("Country").agg(
        *[sum(col_name).alias(col_name) for col_name in financial_columns]
    ).orderBy("Country")  # Sorting in ascending order

    # Show the transformed dataset
    df_grouped.show()

    # Write the resulting DataFrame to Parquet in overwrite mode
    df_grouped.write.option("header", "true").mode("overwrite").parquet("gs://spark-job-data-bucket/processed-data/groupbykey-data/")

except Exception as e:
    # Handle any exceptions or errors
    print("Error occurred:", str(e))

finally:
    # Stop SparkSession
    spark.stop()
