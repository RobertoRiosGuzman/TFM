# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 16:25:25 2022

@author: robertorg
"""
import findspark
findspark.find()
#    Spark Streaming
#    Kafka
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,FloatType,IntegerType,StringType, DoubleType

from pyspark.ml import Pipeline, Transformer, PipelineModel
from pyspark.ml.feature import MinMaxScaler

from pyspark.sql import DataFrame
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 pyspark-shell'
states = ["ACC","CON","FIN","INT","REQ"]
protos = ["tcp","udp"]
services = ["dhcp","dns","ftp","ftp-data",
          "http","irc","pop3","radius","smtp","snmp","ssh"
          ,"ssl"]
def transform(name, df,filas):
    #categories = df.select(self.name).distinct().rdd.flatMap(lambda x : x).collect()
    categories = filas
    for category in categories:
        cat = category
        function = udf(lambda item: 1 if item == cat else 0, IntegerType())
        new_column_name = name +'_'+cat
        df = df.withColumn(new_column_name, function(col(name)))
        
    return df
    
#conf = SparkConf().setAppName("PySpark App").setMaster("local")
#sc = SparkContext(conf=conf).getOrCreate()
#sc.setLogLevel("WARN")
#scc = StreamingContext(sc,60)
spark = SparkSession \
    .builder \
    .appName("Streaming kafka") \
    .config("spark.sql.streaming.metricsEnabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# see the default schema of the dataframe
df_schema = StructType() \
    .add("id", IntegerType()) \
    .add("dur", FloatType()) \
    .add("proto", StringType()) \
    .add("service", StringType()) \
    .add("state", StringType()) \
    .add("spkts", IntegerType()) \
    .add("dpkts", IntegerType()) \
    .add("sbytes", IntegerType()) \
    .add("dbytes", IntegerType()) \
    .add("rate", FloatType()) \
    .add("sttl", IntegerType()) \
    .add("dttl", IntegerType()) \
    .add("sload", FloatType()) \
    .add("dload", FloatType()) \
    .add("sloss", IntegerType()) \
    .add("dloss", IntegerType()) \
    .add("sinpkt", FloatType()) \
    .add("dinpkt", FloatType()) \
    .add("sjit", FloatType()) \
    .add("djit", FloatType()) \
    .add("swin", IntegerType()) \
    .add("stcpb", IntegerType()) \
    .add("dtcpb", IntegerType()) \
    .add("dwin", IntegerType()) \
    .add("tcprtt", FloatType()) \
    .add("synack", FloatType()) \
    .add("ackdat", FloatType()) \
    .add("smean", IntegerType()) \
    .add("dmean", IntegerType()) \
    .add("trans_depth", IntegerType()) \
    .add("response_body_len", IntegerType()) \
    .add("ct_srv_src", IntegerType()) \
    .add("ct_state_ttl", IntegerType()) \
    .add("ct_dst_ltm", IntegerType()) \
    .add("ct_src_dport_ltm", IntegerType()) \
    .add("ct_dst_sport_ltm", IntegerType()) \
    .add("ct_dst_src_ltm", IntegerType()) \
    .add("is_ftp_login", IntegerType()) \
    .add("ct_ftp_cmd", IntegerType()) \
    .add("ct_flw_http_mthd", IntegerType()) \
    .add("ct_src_ltm", IntegerType()) \
    .add("ct_srv_dst", IntegerType()) \
    .add("is_sm_ips_ports", IntegerType()) \
    .add("attack_cat", StringType()) \
    .add("label", IntegerType())
#Con el modelo de entrenamiento guardamos el pipeline para poder preprocesar datos.
dataset = spark.read.csv("C:/Users/robertorg/Documentos/UPM/Master/TFM/datasets/UNSW-NB15 - CSV Files/a part of training and testing set/UNSW_NB15_training-set.csv"
                         ,schema=df_schema,header=True)
df_3 = dataset \
       .withColumn("service", regexp_replace('service', '-', "")) 
df_4 = df_3.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df_3.columns])
df_4 = df_4.na.drop(how="any") 
df_5 = transform("proto", df_4,protos)
df_5 = transform("service", df_5,services)
df_5 = transform("state", df_5,states)

df_5.printSchema()

#Seleccionamos las lista que son números
# get string
flt_cols = [f.name for f in df_5.schema.fields if isinstance(f.dataType, type(FloatType()))]
int_cols = [f.name for f in df_5.schema.fields if isinstance(f.dataType, type(IntegerType()))]
#dbl_cols = [f.name for f in df_5.schema.fields if isinstance(f.dataType, DoubleType)]
# ['colb']
for name in flt_cols:
    print(name)
    mmScaler = MinMaxScaler(inputCol=name, outputCol=name)
    modelScaler = mmScaler.fit(df_5)
    df_5 = mmScaler.transform(df_5)
df_5.printSchema
# view the transformed data
df_5.show(1)
df_5.printSchema()
#pipeline_model.write().overwrite().save("./pipeline.bin")

#my_pipeline = PipelineModel.load("./pipeline.bin")
