from pyspark.sql import SparkSession

if __name__ == "__main__":
    #Inititalize SparkSession
    spark = SparkSession.builder.appName("MypySparkJob").getOrCreate()

    try:
        ##Specify the input file Path
        input_file = 'https://storage.cloud.google.com/spark-job-data-bucket/data/year-2025/month-01/Financial.csv'

        df = spark.read.csv(input_file)
        df.show()
        df.write.option("header","true").mode("overwrite").parquet("s3://data-source-2025-jan/processed-out-data")

        #stop SparkSession
        spark.stop()

    except Exception as e:
        #Handle any exceptions or errors
        print("Error occurred:", str(e))
        spark.stop()