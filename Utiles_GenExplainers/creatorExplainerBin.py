# -*- coding: utf-8 -*-
"""
Created on Tue Feb 22 08:29:46 2022

@author: robertorg

Este programa permitirá crear un explicador SHAP y guardarlo en un path

"""
import sys
import shap
from tkinter import filedialog
import tkinter
import pandas as pd
from joblib import dump, load
def loadFile():
    root = tkinter.Tk() #esto se hace solo para eliminar la ventanita de Tkinter 
    root.withdraw() #ahora se cierra 
    path =filedialog.askopenfilename() #abre el explorador de archivos y guarda la seleccion en la variable!
    return path
def selectorRepositorio():
    root = tkinter.Tk()
    root.withdraw()
    current_directory = filedialog.askdirectory()
    return current_directory
def menuTipoModeloExplainerBin():
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
    print("Selecciona el tipo de modelo del que se quiere generar un explainer.") 
    tipo_modelo_bin = ""
    while tipo_modelo_bin == "":
        print ("1. XGB")
        print ("2. Decision tree")
        print ("3. Random Forest")
        print ("4. KNN")
        print ("5. Logistic Regression")
        print ("6. MLP")
        print ("7. Salir")
         
        print ("Elige una opcion")
     
        opcion = pedirNumeroEntero()
     
        if opcion == 1:
            tipo_modelo_bin = "XGB"
        elif opcion == 2:
            tipo_modelo_bin = "DT"
        elif opcion == 3:
            tipo_modelo_bin = "RF"
        elif opcion == 4:
            tipo_modelo_bin = "KNN"
        elif opcion == 5:
            tipo_modelo_bin = "LogR"
        elif opcion == 6:
            tipo_modelo_bin = "MLP"         
        elif opcion == 7:
            tipo_modelo_bin = "Exit"
        else:
            print ("Introduce un numero entre 1 y 7")
    return tipo_modelo_bin
 
if __name__ == "__main__":
    print("Programa para crear explainers de los modelos. En primer lugar se pedirá "
          +"el tipo de modelo, en segundo lugar habrá que indicar el modelo a cargar, "+
          "por último, según el tipo modelo elegido será necesario indicar el fichero CSV "+
          "con los datos utilizados para el entrenamiento.")
    tipo_modelo_bin = menuTipoModeloExplainerBin()
    if tipo_modelo_bin == "Exit":
        sys.exit()
    #Selector de archivo con el model.
    print("Selecciona modelo.")
    path_model = loadFile()
    model = load(path_model)
    #Selector carpeta con explainer final.
    print("Selecciona directorio donde guardar explainer final.")
    path_repositorio = selectorRepositorio()
    print("Calculando...")
    if tipo_modelo_bin == "XGB":
        explainer = shap.TreeExplainer(model)
    elif tipo_modelo_bin == "DT":
        explainer = shap.TreeExplainer(model)
    elif tipo_modelo_bin == "RF":
        explainer = shap.TreeExplainer(model)
    elif tipo_modelo_bin == "KNN":
        print("Seleccionar dataset con valores X del entrenamiento (necesario para generación del explainer)")
        path_datos_entrenamiento = loadFile()
        X_train = pd.read_csv(path_datos_entrenamiento) # ,sep=';')
        explainer = shap.KernelExplainer(model.predict_proba, shap.kmeans(X_train, int(len(X_train) * 0.01)))
    elif tipo_modelo_bin == "LogR":
        print("Seleccionar dataset con valores X del entrenamiento (necesario para generación del explainer)")
        path_datos_entrenamiento = loadFile()
        X_train = pd.read_csv(path_datos_entrenamiento) # ,sep=';')
        explainer = shap.Explainer(model, X_train)
    elif tipo_modelo_bin == "MLP":
        print("Seleccionar dataset con valores X del entrenamiento (necesario para generación del explainer)")
        path_datos_entrenamiento = loadFile()
        X_train = pd.read_csv(path_datos_entrenamiento) # ,sep=';')
        explainer = shap.KernelExplainer(model.predict_proba , shap.kmeans(X_train, int(len(X_train) * 0.01)))        
    print("Explainer calculado...")
    nombre_fichero = input("Introducir nombre del explainer: ")
    dump(explainer, path_repositorio+"/"+ nombre_fichero + ".joblib")
    print("Proceso realizado.")