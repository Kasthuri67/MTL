from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

inputDirectory = "C:/Users/kasth/input/"

# Create the SparkSession
spark = SparkSession.\
    builder.\
    appName("sparkstream").\
    getOrCreate()

fileSchema = (StructType([
                       StructField("userID", IntegerType(), True),
                       StructField("name", StringType(), True),
                       StructField("age",IntegerType(), True),
                       StructField("friends",IntegerType(), True),
                        ]))

inputDF = (spark
        .readStream
        .format("csv")
        .schema(fileSchema)
        .option("maxFilesPerTrigger", 1)
        .csv(inputDirectory))

outputDir = "C:/Users/kasth/output/"
checkpointDir = "C:/Users/kasth/checkpoint/"
# inputDF.show()
resultDF = inputDF #.select("name", "friends").where(inputDF.age < 30)

# resultDF.show()

streamingQuery = (resultDF.writeStream
    .format("csv")
    .outputMode("append")
    .option("path", outputDir)
    .option("checkpointLocation", checkpointDir)
    .start())
print('at end')

streamingQuery.awaitTermination()