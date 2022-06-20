# -*- coding: utf-8 -*-
"""
Created on Tue Feb 15 16:11:59 2022

@author: robertorg
"""
from model import model_binary
#Escucha en un consumer escuchar kafka y guardar en un df
df = ""
managerModelBinary = model_binary("model_path","explainer_path")
managerModelBinary.predict(df) #ver q hacer con el resultado
#Enviar por otro topic con un producer.