from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import threading
from pyspark.sql import SparkSession


def stop_query_after_timeout(query, timeout):
    def stop_query():
        query.stop()

    timer = threading.Timer(timeout, stop_query)
    timer.start()

    try:
        query.awaitTermination()
    except Exception as e:
        print(f"Exception while waiting for termination: {e}")
    finally:
        timer.cancel()


# Define your S3 credentials
aws_access_key_id = "AKIA2HTZEBEIHOXL5I6Y"
aws_secret_access_key = "YvHSIwA2y53TGNAt2M8Cj6P0K3qT+MtbdrobDFJE"

# Define your S3 bucket and path
s3_bucket = "mtl-bucket"
s3_path = "s3a://{}/customer-reviews".format(s3_bucket)

spark = SparkSession.builder \
    .appName("S3StreamingExample") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

fileSchema = StructType([
    StructField("url", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("reviewer_name", StringType(), True),
    StructField("review_title", StringType(), True),
    StructField("review_rating", StringType(), True),
    StructField("verified_purchase", StringType(), True),
    StructField("review_date", StringType(), True),
    StructField("helpful_count", StringType(), True),
    StructField("uniq_id", StringType(), True),
])

inputDF = (spark
           .readStream
           .format("csv")
           .schema(fileSchema)
           .option("header", "true")  # If your CSV file has a header
           .option("maxFilesPerTrigger", 1)  # Process one file at a time
           .load(s3_path))

query = inputDF.writeStream.outputMode("append").format("console").start()

query.awaitTermination()

# Set the timeout in seconds
timeout_seconds = 120

# Wait for termination with timeout
stop_query_after_timeout(query, timeout_seconds)
