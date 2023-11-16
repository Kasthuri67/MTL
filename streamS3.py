from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define your S3 credentials
aws_access_key_id = "AKIA2HTZEBEIHOXL5I6Y"
aws_secret_access_key = "YvHSIwA2y53TGNAt2M8Cj6P0K3qT+MtbdrobDFJE"

# Define your S3 bucket and path
s3_bucket = "mtl-bucket"
s3_path = "s3a://{}/customer-reviews".format(s3_bucket)

# Define Snowflake options
snowflake_options = {
    "sfURL": 'https://nerikxb-nr77585.snowflakecomputing.com',
    "sfDatabase": 'MTL_DEV_DB',
    "sfWarehouse": 'compute_wh',
    "sfSchema": 'MTL_BRONZE',
    "sfRole": 'ACCOUNTADMIN',
    "sfUser": 'kasthuri',
    "sfPassword": 'FlyHigh1!',
    "error_on_column_count_mismatch": 'false',
}

spark = SparkSession.builder \
    .appName("ReviewStreamApplication") \
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
    StructField("review_text", StringType(), True),
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
           .option("header", "true")
           .option("maxFilesPerTrigger", 1)
           .load(s3_path))

checkpointDir = "C:/Users/kasth/checkpoint/"

streamingQuery = inputDF.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write\
        .format("snowflake")\
        .options(**snowflake_options)\
        .option("dbtable", 'MTL_DEV_DB.MTL_BRONZE.BZ_CUSTOMER_REVIEWS')\
        .mode("append")\
        .save()) \
    .outputMode("append")\
    .start()\

# Wait for the termination of the query
spark.streams.awaitAnyTermination()