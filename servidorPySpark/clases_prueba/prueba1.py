# -*- coding: utf-8 -*-
"""
Created on Fri Feb 18 12:09:26 2022

@author: robertorg
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_TOPIC_NAME_CONS = "test"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "test_1"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

from pyspark import SparkContext, SparkConf
#    Spark Streaming
#    Kafka
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,FloatType,IntegerType,StringType
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import StringIndexer
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 pyspark-shell'

if __name__ == "__main__":
    import findspark
    findspark.find()
    sc = SparkContext().getOrCreate()
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("a") \
        .master("local") \
        .getOrCreate()
    """
        .config("spark.jars", "C:/Users/robertorg/Documentos/UPM/Master/TFM/app/servidorModeloPyspark-sql-kafka-0-10_2.11-2.4.0.jar,C:/Users/robertorg/Documentos/UPM/Master/TFM/app/servidorModeloPy/kafka-clients-1.1.0.jar") \
        .config("spark.executor.extraClassPath", "C:/Users/robertorg/Documentos/UPM/Master/TFM/app/servidorModeloPyspark-sql-kafka-0-10_2.11-2.4.0.jar,C:/Users/robertorg/Documentos/UPM/Master/TFM/app/servidorModeloPy/kafka-clients-1.1.0.jar") \
        .config("spark.executor.extraLibrary", "C:/Users/robertorg/Documentos/UPM/Master/TFM/app/servidorModeloPyspark-sql-kafka-0-10_2.11-2.4.0.jar,C:/Users/robertorg/Documentos/UPM/Master/TFM/app/servidorModeloPy/kafka-clients-1.1.0.jar") \
        .config("spark.driver.extraClassPath", "C:/Users/robertorg/Documentos/UPM/Master/TFM/app/servidorModeloPyspark-sql-kafka-0-10_2.11-2.4.0.jar,C:/Users/robertorg/Documentos/UPM/Master/TFM/app/servidorModeloPy/kafka-clients-1.1.0.jar") \
    """
    spark.sparkContext.setLogLevel("WARN")

    # Construct a streaming DataFrame that reads from testtopic
    transaction_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test") \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()

    transaction_detail_df1 = transaction_detail_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the transaction_detail data
    transaction_detail_schema = StructType() \
        .add("transaction_id", StringType()) \
        .add("transaction_card_type", StringType()) \
        .add("transaction_amount", StringType()) \
        .add("transaction_datetime", StringType())

    transaction_detail_df2 = transaction_detail_df1\
        .select(from_json(col("value"), transaction_detail_schema).alias("transaction_detail"), "timestamp")

    transaction_detail_df3 = transaction_detail_df2.select("transaction_detail.*", "timestamp")

    # Simple aggregate - find total_transaction_amount by grouping transaction_card_type
    transaction_detail_df4 = transaction_detail_df3.groupBy("transaction_card_type")\
        .agg({'transaction_amount': 'sum'}).select("transaction_card_type", \
        col("sum(transaction_amount)").alias("total_transaction_amount"))

    print("Printing Schema of transaction_detail_df4: ")
    transaction_detail_df4.printSchema()

    transaction_detail_df5 = transaction_detail_df4.withColumn("key", lit(100))\
                                                    .withColumn("value", concat(lit("{'transaction_card_type': '"), \
                                                    col("transaction_card_type"), lit("', 'total_transaction_amount: '"), \
                                                    col("total_transaction_amount").cast("string"), lit("'}")))

    print("Printing Schema of transaction_detail_df5: ")
    transaction_detail_df5.printSchema()

    # Write final result into console for debugging purpose
    trans_detail_write_stream = transaction_detail_df5 \
        .writeStream \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    trans_detail_write_stream_1 = transaction_detail_df5 \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS INT)") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
        .outputMode("update") \
        .option("checkpointLocation", "C:/Users/robertorg/Documentos/spark/chek") \
        .start()

    trans_detail_write_stream.awaitTermination()

    print("PySpark Structured Streaming with Kafka Demo Application Completed.")