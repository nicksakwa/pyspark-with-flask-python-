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
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width-device-width, initial-scale=1.0">
            <title>PySpark Flask App</title>
            <style>
                body {font-family: sans-serif; margin : 20px;}
                h1 {color: #333;}
                form { margin-top: 20px;}
                input[type="submit"] { padding: 10px 20px; background-color: #007bff; color: white; border: none; cursor: pointer; }
                .result { margin-top: 30px; padding: 15px; border: 1px solid #ccc; background-color: #f9f9f9; }
                pre { background-color: #eee; padding: 10px; border-radius: 5px; overflow-x: auto; }
            </style>
            <body>
                <h1> Pyspark Flask Example</h1>
                <p> This app demonstrates running a simple Pyspark job from a Flask end point </p>
                <h2>Word count</h2>
                <form action="/word_count" method="post">
                        <label for="test_input">Enter text:</label><br>
                        <textarea id="test_input" name="text_input" rows="5" cols="50">
                                  Hello Spark
                                  Hello Pyspark
                                  Spark is great
                                  Pyspark is awesome
                        </textarea><br><br>
                        <input type="submit" value="Run word count">
                        </form>
                                  
                        {% if word_count_results %}
                        <div class="results">
                            <h3>Word count results:</h3>
                            <pre>{{ word_count_results }}</pre>
                        </div>
                        
                        {% endif %}        
                        <h2>Generate random numbers (PySpark)</h2>
                        <form action="/generate_numbers" method="post">
                            <label for="num_rows">Number of rows:</label>
                            <input type="number" id="num_rows" name="num_rows" value="100000"><br><br>
                            <input type="submit" value="Generate and process">
                        </form>
                            {% if random_number_results %}
                            <div class="result">
                                  <h3>Random numbers processing results:</h3>
                                  <pre>{{ random_number_results }}</pre>
                            </div>
                            {% end if %}
                        </body>
                        </html>
                        """,
                        word_count_results=request.args.get('wc_results')
                        random_number_results=request.args.get('rn_result')
    )
                                  
                                                   
                                  
                                  
                                  
                                  
                                  
                                  
                                  )