
import os,sys,path,sysconfig

from flask import Flask,render_template
from flask import request
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf ,SQLContext
from app import create_app
# from functions import *
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# sys.path.append(r"C:\Users\shefali.agrawal\Downloads\Untitled Folder\flask_pyspark\winutills") 



def init_spark_context():
    # load spark context
    # conf = SparkConf().setAppName("movie_recommendation-server")
    # IMPORTANT: pass aditional Python modules to each worker
    # sc = SparkContext(conf=conf)
    spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
    sc=spark.sparkContext

    
 
    return spark,sc

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    spark ,sc= init_spark_context()
    # dataset_path = os.path.join('static', '')
    # print(dataset_path)
    app = create_app(spark,sc)
    # app.jinja_env.cache = 'null'
    
    app.run(host='127.0.0.1', port=port, debug=False, use_reloader=False)

