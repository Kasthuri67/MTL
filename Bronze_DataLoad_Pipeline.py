from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import *
from datetime import datetime
import snowflake.connector

# Create SparkSession
spark = SparkSession.builder \
    .appName('SparkSession') \
    .config("spark.driver.extractClassPath",
            r"C:\Users\kasth\sparkwd\spark-3.4.1\jars\mssql-jdbc-12.4.1.jre11.jar") \
    .getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

# sql parameters
src_server_name = '192.168.0.231'
src_database_name = 'MTL_DB'
src_schema_name = 'dbo'

# sql connection auth parameters
user = 'sa'
password = 'Kasmo@123'

# SQL configuration
sql_server = f"jdbc:sqlserver://{src_server_name}:1433;" \
             f"databaseName={src_database_name};encrypt=true;trustServerCertificate=true;useNTLMV2=true;"

# Snowflake connection parameters
snowflake_config = {
    'sfURL': 'https://nerikxb-nr77585.snowflakecomputing.com',
    'sfWarehouse': 'compute_wh',
    'sfDatabase': 'MTL_DEV_DB',
    'sfSchema': 'MTL_BRONZE',
    'sfAccount': 'NERIKXB-NR77585',
    'sfUser': 'kasthuri',
    'sfPassword': 'FlyHigh1!',
    'sfRole': 'ACCOUNTADMIN',
    'sfHistory': 'INGESTION_HISTORY_TABLE'
}

# Define Snowflake options
snowflake_options = {
    "sfURL": snowflake_config["sfURL"],
    "sfDatabase": snowflake_config["sfDatabase"],
    "sfWarehouse": snowflake_config["sfWarehouse"],
    "sfSchema": snowflake_config["sfSchema"],
    "sfRole": snowflake_config["sfRole"],
    "sfUser": snowflake_config["sfUser"],
    "sfPassword": snowflake_config["sfPassword"],
}

# source - target table mapping
table_mappings = [{'ORDER_DETAILS': 'BZ_ORDERDETAILS'}]


def populate_ingestion_history_table(src_table, trgt_table, start, end, _status, _error, src_count, trgt_count):
    ingest_schema = StructType([
        StructField("RUN_ID", StringType(), False),
        StructField("RUN_START", TimestampType(), True),
        StructField("RUN_END", TimestampType(), True),
        StructField("DURATION", StringType(), True),
        StructField("SOURCE_SERVER_NAME", StringType(), True),
        StructField("SOURCE_DATABASE_NAME", StringType(), True),
        StructField("SOURCE_SCHEMA_NAME", StringType(), True),
        StructField("SOURCE_TABLE_NAME", StringType(), True),
        StructField("TARGET_DATABASE_NAME", StringType(), True),
        StructField("TARGET_SCHEMA_NAME", StringType(), True),
        StructField("TARGET_TABLE_NAME", StringType(), True),
        StructField("SNOWFLAKE_FLAG", StringType(), True),
        StructField("TRIGGERED_BY", StringType(), True),
        StructField("STATUS", StringType(), True),
        StructField("ERROR", StringType(), True),
        StructField("SOURCE_NBR_OF_RECORDS", StringType(), True),
        StructField("TARGET_NBR_OF_RECORDS", StringType(), True),
        StructField("CREATED_DATE", TimestampType(), True),
        StructField("CREATED_BY", StringType(), True),
        StructField("MODIFIED_DATE", TimestampType(), True),
        StructField("MODIFIED_BY", StringType(), True)
    ])

    run_query = f"SELECT run_id from {snowflake_config['sfHistory']} where CREATED_DATE = (SELECT MAX(CREATED_DATE) from {snowflake_config['sfHistory']}) ;"
    ingest_df = spark.read \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("query", run_query) \
        .load()
    if ingest_df.count() > 0:
        next_value = ingest_df.select("run_id").first()[0] + 1
    else:
        next_value = 1
    ingestion_dict = {
        "RUN_ID": next_value,
        "RUN_START": start,
        "RUN_END": end,
        "DURATION": (end - start).total_seconds(),  # help .total_seconds(),
        "SOURCE_SERVER_NAME": src_server_name,
        "SOURCE_DATABASE_NAME": src_database_name,
        "SOURCE_SCHEMA_NAME": src_schema_name,
        "SOURCE_TABLE_NAME": src_table,
        "TARGET_DATABASE_NAME": snowflake_config['sfDatabase'],
        "TARGET_SCHEMA_NAME": snowflake_config['sfSchema'],
        "TARGET_TABLE_NAME": trgt_table,
        "SNOWFLAKE_FLAG": False,
        "TRIGGERED_BY": snowflake_config["sfUser"],
        "STATUS": _status,
        "ERROR": _error,
        "SOURCE_NBR_OF_RECORDS": src_count,  # help
        "TARGET_NBR_OF_RECORDS": trgt_count,  # help
        "CREATED_DATE": datetime.now(),
        "CREATED_BY": snowflake_config["sfUser"],
        "MODIFIED_DATE": datetime.now(),
        "MODIFIED_BY": snowflake_config["sfUser"]
    }

    ingest_row = [Row(**ingestion_dict)]
    ingest_df = spark.createDataFrame(ingest_row, ingest_schema)

    ingest_df.write \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", f"{snowflake_config['sfSchema']}.{snowflake_config['sfHistory']}") \
        .mode("append") \
        .save()
    print("Ingestion History is updated")


def pipeline():
    print(f"{src_schema_name} : ")

    for mapping in table_mappings:
        source_table = list(mapping.keys())[0]
        target_table = mapping[source_table]

        try:
            print(f"Extracting table: {src_schema_name}.{source_table}")
            run_start = datetime.now()
            ingestion_df = spark.read \
                .format("snowflake") \
                .options(**snowflake_options) \
                .option("dbtable", snowflake_config['sfHistory']) \
                .load()

            history_df = ingestion_df.filter(
                (ingestion_df['SOURCE_SERVER_NAME'] == src_server_name) &
                (ingestion_df['SOURCE_DATABASE_NAME'] == src_database_name) &
                (ingestion_df['SOURCE_SCHEMA_NAME'] == src_schema_name) &
                (ingestion_df['SOURCE_TABLE_NAME'] == source_table) &
                (ingestion_df['TARGET_DATABASE_NAME'] == snowflake_config['sfDatabase']) &
                (ingestion_df['TARGET_SCHEMA_NAME'] == snowflake_config['sfSchema']) &
                (ingestion_df['TARGET_TABLE_NAME'] == target_table) &
                (ingestion_df['STATUS'] == 'SUCCESS')
            )
            row_count = history_df.count()
            print('row_count:', row_count)
            if row_count > 0:  # incremental load
                print('------------Incremental Load------------')
                max_date = history_df.agg(max("RUN_START")).collect()[0][0]
                sql_query = f"(SELECT * FROM {src_schema_name}.{source_table} WHERE LastModifiedDate > '{max_date}') AS filtered_data"

                source_df = spark.read.format('jdbc') \
                    .option('url', sql_server) \
                    .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver') \
                    .option('dbtable', sql_query) \
                    .option('user', user).option('password', password) \
                    .load()

            else:  # full load
                print('------------full load------------')
                run_start = datetime.now()

                source_df = spark.read.format('jdbc') \
                    .option('url', sql_server) \
                    .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver') \
                    .option('dbtable', f"{src_schema_name}.{source_table}") \
                    .option('user', user).option('password', password) \
                    .load()

            source_df = source_df.limit(3)
            source_df = source_df.drop("LastModifiedDate")
            source_df = source_df.withColumn('INSERTED_DATE', current_timestamp()) \
                .withColumn('UPDATED_DATE', current_timestamp())
            src_row_count = source_df.count()
            trgt_ts = source_df.select("INSERTED_DATE").first()[0]
            source_df.write \
                .format("snowflake") \
                .options(**snowflake_options) \
                .option("dbtable", target_table) \
                .mode("append") \
                .save()

            run_end = datetime.now()
            status = 'SUCCESS'

            trgt_query = f"(SELECT COUNT(*) AS value FROM {snowflake_config['sfSchema']}.{target_table} WHERE INSERTED_DATE >= '{trgt_ts}');"
            trgt_rows = spark.read \
                .format("snowflake") \
                .options(**snowflake_options) \
                .option("query", trgt_query) \
                .load()

            trgt_row_count = trgt_rows.select("value").first()[0]
            populate_ingestion_history_table(source_table, target_table, run_start, run_end, status, '', src_row_count,
                                             trgt_row_count)
            print(
                f"Ingestion of source:{src_schema_name}.{source_table} to target:{snowflake_config['sfSchema']}.{target_table} is completed")
            spark.stop()

        except Exception as e:
            print(f"Error while data logic: {e}")
            run_end = datetime.now()
            status = 'FAILED'
            populate_ingestion_history_table(source_table, target_table, run_start, run_end, status, e,
                                             src_row_count, '')


if __name__ == "__main__":
    pipeline()
