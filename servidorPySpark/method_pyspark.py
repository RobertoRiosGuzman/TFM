# -*- coding: utf-8 -*-
"""
Created on Mon Feb 14 18:21:49 2022

@author: robertorg
"""
import findspark

from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
from pyspark.sql.types import  StructType,FloatType,IntegerType,StringType, DoubleType
import pandas as pd
from joblib import load, dump
import sys
rutaRaiz_bin = "./models/binary/"
rutaRaiz_mult = "./models/multiclass/"
extension = ".joblib"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 pyspark-shell'
states = []
protos = []
services = []
col_bin = []
col_mult = []
df_schema = StructType()

def write_mongo_row(df, epoch_id):
    mongoURL = "mongodb://localhost:27017/spark.collection"
    df.write.format("mongo").mode("append").option("uri",mongoURL).save()
    pass 
def make_predictions_bin(sc, df, df_sol, model_path):
    """
    Make predictions.
    
    Arguments:
        sc: SparkContext.
        df (pyspark.sql.DataFrame): Input data frame containing feature_cols.
        feature_cols (list[str]): List of feature columns in df.
        model_path (str): Path to model on Spark driver
    Returns:
        df (pyspark.sql.DataFrame): Output data frame with probability column.
        """
    clf = sc.broadcast(load(model_path))
    
    # Define Pandas UDF
    @F.pandas_udf(returnType=DoubleType(), functionType=F.PandasUDFType.SCALAR)
    def predict(*cols):
        # Columns are passed as a tuple of Pandas Series'.
        # Combine into a Pandas DataFrame
        X = pd.concat(cols, axis=1)
        X.columns = col_bin
        # Make predictions and select probabilities of positive class (1).
        predictions = clf.value.predict(X)[:]
        # Return Pandas Series of predictions.
        return pd.Series(predictions)

    # Make predictions on Spark DataFrame.
    df_sol = df_sol.withColumn("predictions", F.lit(predict(*df)))
    
    return df_sol
def make_predictions_multi(sc, df_predic, df_sol, model_path):
    """
    Make predictions.
    
    Arguments:
        sc: SparkContext.
        df (pyspark.sql.DataFrame): Input data frame containing feature_cols.
        feature_cols (list[str]): List of feature columns in df.
        model_path (str): Path to model on Spark driver
    Returns:
        df (pyspark.sql.DataFrame): Output data frame with probability column.
        """
    clf = sc.broadcast(load(model_path))
    
    # Define Pandas UDF
    @F.pandas_udf(returnType=DoubleType(), functionType=F.PandasUDFType.SCALAR)
    def predict(*cols):
        # Columns are passed as a tuple of Pandas Series'.
        # Combine into a Pandas DataFrame
        X = pd.concat(cols, axis=1)
        X.columns = col_mult
        # Make predictions and select probabilities of positive class (1).
        predictions = clf.value.predict(X)[:]
        # Return Pandas Series of predictions.
        return pd.Series(predictions)

    # Make predictions on Spark DataFrame.
    df_sol = df_sol.withColumn("predictions_multi", F.lit(predict(*df_predic)))
    
    return df_sol

def transform(name, df,filas):
    categories = filas
    for category in categories:
        cat = category
        function = F.udf(lambda item: 1 if item == cat else 0, IntegerType())
        new_column_name = name +'_'+cat
        df = df.withColumn(new_column_name, function(F.col(name)))
    return df
def menu_bin():
    def pedirNumeroEntero():
     
        correcto=False
        num=0
        while(not correcto):
            try:
                num = int(input("Introduce un numero entero: "))
                correcto=True
            except ValueError:
                print('Error, introduce un numero entero')
         
        return num
 
    opcion = 0
    print("Selecciona el modelo a utilizar para clasificar el tráfico como anomalía.") 
    path_model_bin = ""
    while path_model_bin == "":
        print ("1. XGB")
        print ("2. Decision tree")
        print ("3. Random Forest")
        print ("4. MLP")
        print ("5. Logistic Regression")
        print ("6. KNN")
        print ("7. Salir")
         
        print ("Elige una opcion")
     
        opcion = pedirNumeroEntero()
     
        if opcion == 1:
            path_model_bin = "XGB"
        elif opcion == 2:
            path_model_bin = "DT"
        elif opcion == 3:
            path_model_bin = "RF"
        elif opcion == 4:
            path_model_bin = "MLP"
        elif opcion == 5:
            path_model_bin = "LogR"
        elif opcion == 6:
            path_model_bin = "KNN"         
        elif opcion == 7:
            path_model_bin = "Exit"
        else:
            print ("Introduce un numero entre 1 y 7")
    return path_model_bin
def menu_mult():
    def pedirNumeroEntero():
     
        correcto=False
        num=0
        while(not correcto):
            try:
                num = int(input("Introduce un numero entero: "))
                correcto=True
            except ValueError:
                print('Error, introduce un numero entero')
         
        return num
 
    opcion = 0
    print("Selecciona el modelo a utilizar para clasificar el tráfico como anomalía.") 
    path_model_mult = ""
    while path_model_mult == "":
        print ("1. XGB")
        print ("2. Decision tree")
        print ("3. Random Forest")
        print ("4. MLP")
        print ("5. Logistic Regression")
        print ("6. KNN")
        print ("7. Salir")
         
        print ("Elige una opcion")
     
        opcion = pedirNumeroEntero()
     
        if opcion == 1:
            path_model_mult = "XGB"
        elif opcion == 2:
            path_model_mult = "DT"
        elif opcion == 3:
            path_model_mult = "RF"
        elif opcion == 4:
            path_model_mult = "MLP"
        elif opcion == 5:
            path_model_mult = "LogR"
        elif opcion == 6:
            path_model_mult = "KNN"                
        elif opcion == 7:
            path_model_mult = "Exit"
        else:
            print ("Introduce un numero entre 1 y 7")
    return path_model_mult
def inicializar_config():
    import json
    global df_schema,states,protos ,services ,col_bin ,col_mult
    with open('./config/config.json','r') as jsonfile:
        config = json.loads(jsonfile.read().replace("ï»¿",""))
    df_schema = StructType.fromJson(config["DF_SCHEMA_INPUT"])
    print(df_schema)
    states = config["STATES"]
    print(states)
    protos = list(config["PROTO"])
    print(protos)
    services = config["SERVICES"]
    print(services)
    col_bin = config["FEATURES_BIN"]
    print(col_bin)
    col_mult = config["FEATURES_MULTI"]
    print(col_mult)
    #database_password = config['DEFAULT']['DB_PASSWORD']
if __name__ == "__main__":
    """
        Se elegirá tipo del modelo que se utilzará para analizar las trazas de tráfico.
    """
    path_model_bin = menu_bin()
    if path_model_bin == "Exit":
        sys.exit()
    path_model_multi = menu_mult()
    if path_model_multi == "Exit":
        sys.exit()        
    inicializar_config()
    findspark.find()
    spark = SparkSession \
        .builder \
        .appName("Streaming kafka") \
        .master("local[2]") \
        .config("spark.executor.heartbeatInterval","3600s") \
        .config("spark.network.timeout", "7200s") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test") \
        .option("failOnDataLoss" , "false") \
        .option("startingOffsets", "latest") \
        .load()
    print("Esquema de dataframe de kafka")
    df.printSchema()
    df_1 = df.selectExpr("CAST(value AS STRING)")
    
    df_2 = df_1 \
        .select(F.from_json(F.col("value"),df_schema).alias("data_inputs")) \
        .select("data_inputs.*")#.show()
    print("Esquema de dataframe 2")
    df_2.printSchema()
    #Eliminamos valores - de la columna state 
    df_3 = df_2 \
           .withColumn("service", F.regexp_replace('service', '-', "")) 
    df_4 = df_3.select([F.when(F.col(c)=="",None).otherwise(F.col(c)).alias(c) for c in df_3.columns])
    df_4 = df_4.na.drop(how="any")     
    #Aplicamos One hot Encoder a las columnas string.
    #if "proto" in (col_bin or col_mult):
    df_5 = transform("proto", df_4,protos)
    #if "service" in (col_bin or col_mult):
    df_5 = transform("service", df_5,services)
    #if "state" in (col_bin or col_mult):
    df_5 = transform("state", df_5,states)
    df_5.printSchema()
    df_bin = df_5.select([col for col in df_5.columns if col in col_bin])
    df_mult = df_5.select([col for col in df_5.columns if col in col_mult])
    print("df_bin")
    df_bin.printSchema()
    df_mult.printSchema()
    result = df_5
    
    print("Esquema features: ")
    result.printSchema()
    result = make_predictions_bin(spark.sparkContext, df_bin, result, rutaRaiz_bin+ path_model_bin+extension)
    print("Esquema predicción binaria: ")
    df_bin.printSchema()
    result.printSchema()
    result = make_predictions_multi(spark.sparkContext, df_mult, result, rutaRaiz_mult+ path_model_multi+extension)
    print("Esquema predicción multiclase: ")
    result.printSchema()
    
    """
    result = result.withColumn("key", F.lit(1))\
                                        .withColumn("value", F.concat(F.lit('{"prediction": "'), \
                                        F.col("predictions"), F.lit('", "class_anomaly": "'), \
                                        F.col("predictions_multi").cast("string"), F.lit('", "model_bin": "'), \
                                        F.lit(path_model_bin),F.lit('", "model_mult": "'), \
                                        F.lit(path_model_multi), F.lit('"}'))) 
    """                                        
    result = result \
        .withColumn("model_bin",F.lit(path_model_bin)) \
            .withColumn("model_mult",F.lit(path_model_multi))
                    
    result = result.withColumn("value", F.to_json(F.struct([ x for x in result.columns]))) \
        .withColumn("key", F.lit(1))
    print("Esquema final: ")
    result.printSchema()
    print("df_bin")
    df_bin.printSchema()
    trans_detail_write_stream_1 = result \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "test_1") \
        .outputMode("update") \
        .option("checkpointLocation", "C:/Users/robertorg/Documentos/spark/chek") \
        .start()
    #trans_detail_write_stream_1.awaitTermination()
    query = result \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream.foreachBatch(write_mongo_row).start()
    query.awaitTermination()

    """
    def write_mongo_row(df, epoch_id):
    mongoURL = "mongodb://XX.XX.XX.XX:27017/test.collection"
    df.write.format("mongo").mode("append").option("uri",mongoURL).save()
    pass

query=csvDF.writeStream.foreachBatch(write_mongo_row).start()
query.awaitTermination()
    """
"""
StructType() \
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
    col_results = ["dur", "spkts", "dpkts", "sbytes", "dbytes", "rate", "sttl",
           "dttl", "sload", "dload", "sloss", "dloss", "sinpkt", "dinpkt", "sjit",
           "djit", "swin", "stcpb", "dtcpb", "dwin", "tcprtt", "synack", "ackdat",
           "smean", "dmean", "trans_depth", "response_body_len", "ct_srv_src",
           "ct_state_ttl", "ct_dst_ltm", "ct_src_dport_ltm", "ct_dst_sport_ltm",
           "ct_dst_src_ltm", "is_ftp_login", "ct_ftp_cmd", "ct_flw_http_mthd",
           "ct_src_ltm", "ct_srv_dst", "is_sm_ips_ports",
           "proto_tcp", "proto_udp", "service_dhcp", "service_dns", "service_ftp",
           "service_ftp-data", "service_http", "service_irc", "service_pop3",
           "service_radius", "service_smtp", "service_snmp", "service_ssh",
           "service_ssl", "state_CON", "state_FIN", "state_INT",
           "state_REQ","predictions","predictions_multi","model_mult","model_bin"]
"""
        