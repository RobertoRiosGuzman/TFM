# -*- coding: utf-8 -*-
"""
Created on Wed Feb 23 13:55:42 2022

@author: robertorg
"""

from confluent_kafka import Consumer
import json
from joblib import load
import shap
import pandas as pd
import html
from datetime import datetime
path_rel_bin = "./explainers/binary/"
path_rel_multi = "./explainers/multiclass/"
extension = ".joblib"
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
col_bin = ["dinpkt", "ct_dst_sport_ltm", "sbytes", "dpkts", "dbytes", "ct_src_dport_ltm",
       "dmean", "ct_dst_sport_ltm", "dload", "state_INT",
       "sttl", "ct_state_ttl"]
col_multi = ["sload", "dloss", "dload", "dbytes", "response_body_len", "dmean", "sttl"]
class consumer():
    
    def __init__(self):
        self
        self.topic = "test_1"  
        self.conf = {  
          'bootstrap.servers': "localhost:9092", #usually of the form cell-1.streaming.<region>.oci.oraclecloud.com:9092  
          'group.id': "consumer"
            }  
        self.model_bin = None
        self.logs = []
        self.data = []
        self.anomalys = [{"name":"Analysis", "contador":0},
                    {"name":"Backdoor", "contador":0},
                    {"name":"DoS", "contador":0},
                    {"name":"Exploits", "contador":0},
                    {"name":"Fuzzers", "contador":0},
                    {"name":"Generic", "contador":0},
                    {"name":"Reconnaissance", "contador":0},
                    {"name":"Worms", "contador":0}]
        self.countAnomaly=0
        self.countNoAnomaly = 0
        # Create Consumer instance
        self.start = False
    def run(self):
        import threading
        if self.start == False:
            self.consumer = Consumer(self.conf)
            #t = threading.Thread(target=self.readMsg, name='readMsg')
            self.start = True
            #t.start()
            self.readMsg()
    def isRun(self):
        return self.start
    def readMsg(self):
            
        # Subscribe to topic
        self.consumer.subscribe([self.topic])

        # Process messages
        try:
            while self.start:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    # No message available within timeout.
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    #print("Waiting for message or event/error in poll()")
                    print(self.start)
                    continue
                elif msg.error():
                    print('error: {}'.format(msg.error()))
                else:

                    _,time = msg.timestamp()
                    record_value = json.loads(msg.value().decode("utf-8"))
                    prediccion = record_value["predictions"]
                    typeAnomaly = record_value["predictions_multi"]
                    if int(float(prediccion)) == 1:#Anomalia
                        print("ANOMALIA")
                        self.countAnomaly =self.countAnomaly+ 1
                        self.anomalys[int(float(typeAnomaly))]["contador"]=  self.anomalys[int(float(typeAnomaly))]["contador"] + 1
                        #print(self.anomalys[int(float(typeAnomaly))])
                        self.logs.append({"id":len(self.logs), "time":time,
                                           "isAnomaly":"YES", "type": self.anomalys[int(float(typeAnomaly))]["name"]})
                        self.data.append(record_value)
                    else:#no anomalia
                        print("NO ANOMALIA")
                        self.countNoAnomaly=self.countNoAnomaly+ 1
                        self.logs.append({"id":len(self.logs), "time":time,
                                           "isAnomaly":"NO", "type": "-"})
                        self.data.append(record_value)
                    #print("Consumed record with key "+ record_key + " and value " + record_value)
                    
        except KeyboardInterrupt:
            pass
        finally:
            print("Leave group and commit final offsets")
            self.consumer.close()
    def explainer_bin(self,id):
        #cargamos explainer bin
        registro = pd.DataFrame(self.data[id], index=[0]) #convertimos a pandas
        typeExplainer = registro.model_bin[0]
        prediccion = registro.predictions[0]
        explainer = load(path_rel_bin + typeExplainer+extension)
        serie = registro[col_bin]
        shap_values = explainer.shap_values(serie)
        if (typeExplainer !="KNN" and
            typeExplainer !="MLP"and
                typeExplainer !="RF"):
            p = shap.force_plot(explainer.expected_value, shap_values[0],
                                serie.values[0], feature_names = serie.columns,
                                matplotlib=False)
        else:
            valorMedio = explainer.expected_value[int(float(prediccion))]
            p = shap.force_plot(valorMedio, shap_values[0],
                                serie.values[0], feature_names = serie.columns,
                                matplotlib=False)
        shap_html = f"<head>{shap.getjs()}</head><body>{p.html()}</body>"
        return shap_html
    def explainer_multi(self,id):
        #cargamos explainer multi
        shap_html = ""
        pred = ""
        registro = pd.DataFrame(self.data[id], index=[0]) #convertimos a pandas        typeExplainer = registro.model_multi[0]
        if registro.predictions[0]== 1:
            prediccion = registro.predictions_multi[0]
            typeExplainer = registro.model_mult[0]
            explainer = load(path_rel_multi + typeExplainer+extension)
            serie = registro[col_multi]
            shap_values = explainer.shap_values(serie)
            p = shap.force_plot(explainer.expected_value[int(float(prediccion))],
                                shap_values[int(float(prediccion))][0],
                                serie.values[0], feature_names = serie.columns,
                                matplotlib=False)
            shap_html = f"<head>{shap.getjs()}</head><body>{p.html()}</body>"
            pred = self.anomalys[int(float(prediccion))]["name"]        
        return pred, shap_html
    def stop(self):
        self.start = False
        self.consumer.close()
"""
c = consumer()
c.run()
while True:
    a = input("Intruduce YA")
    print(c.data)
    if a == "YA":
        c.explainer_bin(0)
"""