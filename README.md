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
2.	Apache Kafka (3.1.0): descargamos e instalamos kafka 3.1.0 de la página oficial https://kafka.apache.org/downloads
3.	Spark (3.0.3): de la página oficial de Spark descargamos spark-3.0.3-bin-hadoop2.7 desde https://spark.apache.org/downloads.html, descargamos https://github.com/steveloughran/winutils con los binarios Handoop para Windows y copiamos winutils.exe dentro de la carpeta /bin de spark. Una vez realizado estos pasos añadimos la variable de entorno SPARK_HOME con el path donde hayamos descomprimido spark, JAVA_HOME donde se tenga instalado Java 8 y HADOOP-HOME al directorio raíz de WinUtils alojado dentro de la carpeta /bin. Además, otra variable que hay que modificar es la variable PATH, en la que añadimos el /bin tanto de Spark como de Java 8. Finalmente, el último paso necesario, sería dar permisos de escritura sobre la carpeta c:\tmp\hive. Para ello creamos la carpeta, y ya que hemos descargado WinUtils, utilizamos winutils chmod 777 sobre un terminal que esté abierto como administrador.
4.	Mongodb: instalar mongodb para la versión de windows correspondiente mediante https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-windows/

## Arquitectura de directorios
Este repositorio utiliza la siguiente arquitectura:
- Utiles_GenExplainers: contiene dos scripts para generar los explainers explicativos para cualquier modelo multiclase o binario de tipo Random Forest, Regresión Logística, XGBoost, MLP, KNN y Decision Tree.
- Utiles_GenModelos: cuadernillo jupyter con preprocesamiento, estudio y creación de modelos por defecto, junto con estadísticas de métricas y uso de SHAP.
- simuladorTrazas: script con simulación de monitoreo de trazas.
- servidorPySpark: script desarrollado en PySpark para procesar utilizando los modelos de Machine Learning las trazas recibidas por el topic test, reenviando los resultados por el topic test_1 y almacenando en mongodb.
- servidorFlask: script principal de la aplicación con los archivos html y css correspondientes.
## Tutorial de uso
Se debe seguir los siguientes pasos:
  1. Ejecutar Zookeeper y Kafka: en el directorio batKafka se encuentra los scripts a ejecutar. Primero, initZookeeper para inicializar servidor Zookeeper, en segundo lugar initKafka para inicializar servidor Kafka, crear los topics de comunicación mediante initTopicTest y initTopicTest_1. IMPORTANTE: modificar los script con las rutas de cada sistema propio.
  2. Ejecutar script method_pyspark seleccionando el modelo que se desee para cada tipo de clasificación. Se deberá modificar el fichero config.json en caso de utilizar otros modelos a los alojados en el directorio servidorPySpark\models. Destacar que el modelo RF multiclass no está subido por exceso de peso en el repositorio, pero se podrá generar utilizando el cuadernillo Jupyter alojado en Utiles_GenModelos\generadorModelosDefecto.
  3. Ejecutar script app dentro de servidorFlask que ejecuta el programa principal. Una vez se reciba trazas utilizando el simulador de trazas creado o un sistema real configurado por el usuario (conectado al topic test como productor) empezará a funcionar el sistema. Destacar que dentro de servidorFlask\explainers se encuentran los explicadores por defecto (por falta de espacio de GitHub no se ha subido XGBoost y RF para multiclase), pero se pueden generar los propios explainers que se necesiten desde Utiles_GenExplainers con los script creatorExplainersBin.py o creatorExplainersMult.py.
