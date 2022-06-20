# -*- coding: utf-8 -*-
"""
Created on Mon Feb 14 13:39:14 2022

@author: robertorg
"""

from flask import Flask, render_template, request
from consumidorPy import consumer
from io import BytesIO
import base64

from flask_executor import Executor

# DOCS https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
app = Flask(__name__)

c = consumer() 
executor = None
@app.route('/')
def main():
    global executor
    executor = Executor(app)
    executor.submit(c.run)
    return "Go to principal <a href=""/principal"">page</a>"
@app.route('/explain')
def ex():
    log = request.args.get('idLog')
    plt_bin = c.explainer_bin(int(log))
    pred,plt_multi = c.explainer_multi(int(log))
    return render_template('Explain.html', 
                           result=plt_bin, typeP=pred, 
                           result_multiclass=plt_multi) 

@app.route('/principal')
def peticion():
    print("Refresh: "+str(c.countAnomaly))
    return render_template('Predicts.html', countAnomaly=c.countAnomaly,
                           countNoAnomaly=c.countNoAnomaly,
                           anomalys=c.anomalys, logs=c.logs)
@app.route('/stop')
def stop():
    global executor
    c.stop()
    executor.shutdown()
    return "OK"
    
if __name__ == '__main__': 
    try:
        app.run(host="127.0.0.1", port=5000,debug=True,threaded=True)
    except:
        print("Adios")
    finally:
        #c.stop()
        executor.shutdown()
