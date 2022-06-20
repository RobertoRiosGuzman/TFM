# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 10:23:48 2022

@author: robertorg
"""

from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import json
import random
import pandas as pd
KAFKA_TOPIC_NAME_CONS = "test"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    df_tx = pd.read_csv('C:/Users/robertorg/Documentos/UPM/Master/TFM/datasets/memoria.csv')

    for i in range(150):
        i = i + 1
        message = df_tx[i-1:i].to_json(orient = 'records')
        message_json = json.loads(message)
        print(message_json)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message_json[0])
        time.sleep(3)