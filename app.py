import os
from flask import Flask, render_template_string, request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

app=Flask(__name__)

def get_spark_session():
    spark = SparkSession.builder
        .appName("PySparkFlaskApp")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.executer.memory", "4g")
        .getOrCreate()
    return spark

@app.route('/')
def index():
    return render_template_string("""
                                  
                                  
                                  
                                  
                                  
                                  
                                  )