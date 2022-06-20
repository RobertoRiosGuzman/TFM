# TFM

Debido a las múltiples amenazas cibernéticas contra empresas y usuarios es necesaria la creación de distintas herramientas que les permitan sentirse seguros, sin ver comprometida la integridad de sus dispositivos. Gracias a la evolución de distintas tecnologías, se disponen de soluciones para cumplir esta finalidad detectando posibles amenazas sobres sus equipos en tiempo real pudiendo así combatirlas y bloquearlas. Unas de estas herramientas son los llamados sistemas de detección de intrusos o IDS.
El objetivo de este proyecto es la realización de un IDS utilizando técnicas de Machine Learning para reconocer patrones de información en la red, clasificando y detectando tráfico normal o anómalo, junto con posibles tipos de ataques recibidos. Además, solucionando problemas de confianza y razonamiento, se plantea el uso de un nuevo campo llamado Inteligencia Artificial Explicativa (XAI), indicando una explicación razonable del motivo elegido para realizar una clasificación determinada.
Para completar dicho desafío, se plantea el uso de plataformas como Apache Kafka, la cual permite la conectividad entre todos los elementos de la aplicación, o Spark, que dispone de la posibilidad de analizar cada traza de red en tiempo real con tecnología de Machine Learning, gracias a sus módulos de Spark Streaming o Spark SQL. Se utiliza librerías especializadas para este campo como Scikit-learn o para mostrar explicaciones como SHAP, entrenando los distintos modelos y aprendiendo diferentes ataques gracias al conjunto de datos UNSW-NB15. Finalmente, se busca la completa escalabilidad y modularidad del programa permitiendo personalizaciones por parte de cada usuario que utilice el mismo.
Este proyecto demuestra la compatibilidad de diseñar e implementar un IDS utilizando XAI con unos resultados fiable y precisos a la hora de clasificar anomalías y distintos ataques que se puedan producir en la red a monitorizar.

## Instalación de componentes
Este trabajo se ha realizado en un dispositivo con el sistema operativo Windows 10. Es necesario instalar los siguientes componentes:
1.	Anaconda, Python (3.7) y dependencias: se crea entorno para el proyecto con la versión Python 3.7 y se instala requierements.txt que incluirán SHAP y Flask.
  	```
    conda create -n nombreenv python=3.7
    ```
    ```
    conda activate nombreenv 
    ```
    ```
    pip install requirements.txt
    ```
2.	Apache Kafka (VX):
3.	Spark (VX):
