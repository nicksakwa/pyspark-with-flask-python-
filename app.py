import os
from flask import Flask, render_template_string, request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

# --- PySpark Initialization (Choose ONE method) ---

# Method 1: Using environment variables (recommended for deployment/more controlled environments)
# Ensure SPARK_HOME and PYSPARK_PYTHON are set in your shell before running `flask run` or `python app.py`

# Method 2: Using findspark (more self-contained within Python script)
# You need to replace "YOUR_SPARK_HOME_PATH" with the actual path to your Spark installation.
# import findspark
# try:
#     findspark.init(spark_home="C:/spark/spark-3.5.1-bin-hadoop3") # Example for Windows
#     # findspark.init(spark_home="/opt/spark/spark-3.5.1-bin-hadoop3") # Example for Linux/macOS
# except Exception as e:
#     print(f"Error initializing findspark: {e}. Make sure SPARK_HOME is correct or set manually.")


app = Flask(__name__)

# Function to get or create SparkSession
# It's crucial to manage SparkSession lifecycle.
# In a Flask app, creating a new session per request can be inefficient.
# For simplicity, we'll get a shared session. For production, consider
# a Singleton pattern or pre-initializing it.
def get_spark_session():
    # Use .getOrCreate() to ensure only one SparkSession is created per application
    spark = SparkSession.builder \
        .appName("PySparkFlaskApp") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") # Allocate more memory to driver if needed
        .config("spark.executor.memory", "4g") # Allocate more memory to executors if needed
        .getOrCreate()
    return spark

@app.route('/')
def index():
    return render_template_string("""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>PySpark Flask App</title>
            <style>
                body { font-family: sans-serif; margin: 20px; }
                h1 { color: #333; }
                form { margin-top: 20px; }
                input[type="submit"] { padding: 10px 20px; background-color: #007bff; color: white; border: none; cursor: pointer; }
                .result { margin-top: 30px; padding: 15px; border: 1px solid #ccc; background-color: #f9f9f9; }
                pre { background-color: #eee; padding: 10px; border-radius: 5px; overflow-x: auto; }
            </style>
        </head>
        <body>
            <h1>PySpark Flask Example</h1>
            <p>This app demonstrates running a simple PySpark job from a Flask endpoint.</p>

            <h2>Word Count</h2>
            <form action="/word_count" method="post">
                <label for="text_input">Enter text:</label><br>
                <textarea id="text_input" name="text_input" rows="5" cols="50">
Hello Spark
Hello PySpark
Spark is great
PySpark is awesome
                </textarea><br><br>
                <input type="submit" value="Run Word Count">
            </form>

            {% if word_count_results %}
                <div class="result">
                    <h3>Word Count Results:</h3>
                    <pre>{{ word_count_results }}</pre>
                </div>
            {% endif %}

            <h2>Generate Random Numbers (PySpark)</h2>
            <form action="/generate_numbers" method="post">
                <label for="num_rows">Number of rows:</label>
                <input type="number" id="num_rows" name="num_rows" value="100000"><br><br>
                <input type="submit" value="Generate and Process">
            </form>

            {% if random_number_results %}
                <div class="result">
                    <h3>Random Number Processing Results:</h3>
                    <pre>{{ random_number_results }}</pre>
                </div>
            {% endif %}

        </body>
        </html>
    """,
    word_count_results=request.args.get('wc_results'),
    random_number_results=request.args.get('rn_results')
    )

@app.route('/word_count', methods=['POST'])
def word_count():
    text_input = request.form['text_input']
    spark = get_spark_session()

    # Create a DataFrame from the input text
    lines_rdd = spark.sparkContext.parallelize(text_input.split('\n'))
    words_df = lines_rdd.flatMap(lambda line: line.lower().split(" ")) \
                         .filter(lambda word: word != '') \
                         .toDF(["word"])

    # Perform word count using DataFrame API
    word_counts_df = words_df.groupBy("word").agg(count("*").alias("count"))
    result = word_counts_df.orderBy(col("count").desc()).collect()

    # Format results for display
    results_str = "\n".join([f"{row.word}: {row.count}" for row in result])
    # Don't stop SparkSession if you plan to reuse it
    # spark.stop() # Only call spark.stop() when the application completely shuts down or session is no longer needed.

    return render_template_string(index(), wc_results=results_str)

@app.route('/generate_numbers', methods=['POST'])
def generate_numbers():
    try:
        num_rows = int(request.form.get('num_rows', 100000))
        if num_rows <= 0:
            return "Please enter a positive number of rows."
    except ValueError:
        return "Invalid input for number of rows."

    spark = get_spark_session()

    # Generate a large DataFrame with random numbers
    data = [(i, float(i) * 0.1 + (i % 5)) for i in range(num_rows)]
    df = spark.createDataFrame(data, ["id", "value"])

    # Perform some aggregation
    total_sum = df.agg(sum("value").alias("total_value")).collect()[0]["total_value"]
    avg_value = df.agg(col("value").alias("avg_value")).summary("mean").collect()[0]["mean"] # More direct for mean

    results_str = f"Generated {num_rows} rows.\n" \
                  f"Total sum of values: {total_sum:.2f}\n" \
                  f"Average value: {float(avg_value):.2f}"

    return render_template_string(index(), rn_results=results_str)

if __name__ == '__main__':
    # It's better to explicitly tell Flask where your app is if you're not using `flask run`
    # Set FLASK_APP environment variable: export FLASK_APP=app.py
    # Then run with `flask run`
    # Or, for simple local testing without setting env vars:
    app.run(debug=True, host='0.0.0.0', port=5000)