import os
from flask import Flask, render_template_string, request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

# --- PySpark Initialization Strategy ---
# Choose ONE of the following methods for initializing PySpark.

# Method 1 (Recommended for Development/Testing): Using environment variables
# For this method, you MUST set the following environment variables in your terminal
# BEFORE you run 'python app.py' or 'flask run':
#
# On Linux/macOS:
# export SPARK_HOME="/path/to/spark-3.x.x-bin-hadoopx.x"
# export PYSPARK_PYTHON=$(which python) # Points to your virtual environment's python
#
# On Windows (Command Prompt):
# set SPARK_HOME="C:\path\to\spark-3.x.x-bin-hadoopx.x"
# set PYSPARK_PYTHON=%VIRTUAL_ENV%\Scripts\python.exe
#
# On Windows (PowerShell):
# $env:SPARK_HOME="C:\path\to\spark-3.x.x-bin-hadoopx.x"
# $env:PYSPARK_PYTHON=(Get-Command python).Path
#
# This is generally cleaner as it separates configuration from code.

# Method 2 (More self-contained for scripts, requires 'findspark'):
# Uncomment the lines below if you prefer to set SPARK_HOME within the Python script.
# You need to install findspark: pip install findspark
# Remember to replace "YOUR_SPARK_HOME_PATH" with the actual path to your Spark installation.
# import findspark
# try:
#     # Example for Windows:
#     # findspark.init(spark_home="C:/spark/spark-3.5.1-bin-hadoop3")
#     # Example for Linux/macOS:
#     # findspark.init(spark_home="/opt/spark/spark-3.5.1-bin-hadoop3")
#     pass # Replace this with your findspark.init() call if using this method
# except Exception as e:
#     print(f"Error initializing findspark: {e}. Make sure SPARK_HOME is correct or set manually.")
#     # You might want to exit or raise an error here in a real app if Spark cannot be found.


# --- Flask Application Setup ---

# This line MUST start at the very first column (no indentation)
app = Flask(__name__)

# Function to get or create SparkSession
# It's crucial to manage SparkSession lifecycle.
# In a Flask app, creating a new session per request can be very inefficient.
# By using .getOrCreate(), we ensure that only one SparkSession is created
# for the entire Flask application instance, and subsequent calls reuse it.
def get_spark_session():
    # The '.builder' and subsequent methods ('.appName', '.master', '.config', '.getOrCreate')
    # should be aligned correctly, typically aligned with the start of '.builder'.
    spark = SparkSession.builder \
        .appName("PySparkFlaskApp") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    return spark

# --- Flask Routes ---

@app.route('/')
def index():
    # Use render_template_string for a simple HTML page directly in the Python file.
    # For more complex UIs, consider creating separate .html files in a 'templates' folder
    # and using Flask's 'render_template()' function.
    return render_template_string("""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>PySpark Flask App</title>
            <style>
                body { font-family: sans-serif; margin: 20px; line-height: 1.6; }
                h1, h2 { color: #2c3e50; }
                form { margin-top: 20px; padding: 15px; border: 1px solid #ddd; border-radius: 8px; background-color: #f9f9f9; }
                label { display: block; margin-bottom: 8px; font-weight: bold; }
                textarea, input[type="number"] { width: calc(100% - 20px); padding: 10px; margin-bottom: 15px; border: 1px solid #ccc; border-radius: 4px; }
                input[type="submit"] {
                    padding: 10px 20px;
                    background-color: #007bff;
                    color: white;
                    border: none;
                    border-radius: 5px;
                    cursor: pointer;
                    font-size: 16px;
                }
                input[type="submit"]:hover { background-color: #0056b3; }
                .result { margin-top: 30px; padding: 15px; border: 1px solid #cce5ff; background-color: #e0f2ff; border-left: 5px solid #007bff; border-radius: 8px; }
                pre { background-color: #eee; padding: 10px; border-radius: 5px; overflow-x: auto; white-space: pre-wrap; word-wrap: break-word; }
            </style>
        </head>
        <body>
            <h1>PySpark Flask Example</h1>
            <p>This app demonstrates running simple PySpark jobs from a Flask web interface.</p>

            <h2>Word Count</h2>
            <form action="/word_count" method="post">
                <label for="text_input">Enter text for word count:</label><br>
                <textarea id="text_input" name="text_input" rows="7" cols="60">
Hello Spark, this is a test.
Spark is powerful. PySpark makes it easier.
Test, test, one two three.
                </textarea><br>
                <input type="submit" value="Run Word Count with PySpark">
            </form>

            {% if word_count_results %}
                <div class="result">
                    <h3>Word Count Results:</h3>
                    <pre>{{ word_count_results }}</pre>
                </div>
            {% endif %}

            <h2>Generate and Process Random Numbers (PySpark)</h2>
            <form action="/generate_numbers" method="post">
                <label for="num_rows">Number of rows to generate and process:</label>
                <input type="number" id="num_rows" name="num_rows" value="100000" min="1"><br>
                <input type="submit" value="Generate and Process with PySpark">
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
    # Pass results from URL query parameters after redirecting back to index
    word_count_results=request.args.get('wc_results'),
    random_number_results=request.args.get('rn_results')
    )

@app.route('/word_count', methods=['POST'])
def word_count():
    text_input = request.form['text_input']
    spark = get_spark_session() # Get the existing or create a new SparkSession

    # Create a DataFrame from the input text
    # This example tokenizes words and converts them to lowercase.
    lines_rdd = spark.sparkContext.parallelize(text_input.split('\n'))
    words_df = lines_rdd.flatMap(lambda line: line.lower().split(" ")) \
                         .filter(lambda word: word.strip() != '') \
                         .toDF(["word"])

    # Perform word count using PySpark DataFrame API
    word_counts_df = words_df.groupBy("word").agg(count("*").alias("count"))
    
    # Collect results to the Flask driver. Be cautious with very large datasets,
    # as 'collect()' brings all data to the driver, potentially causing OOM errors.
    # For large results, consider writing to a file/DB and Flask reading from there.
    result = word_counts_df.orderBy(col("count").desc()).collect()

    # Format results for display on the web page
    results_str = "\n".join([f"{row.word}: {row.count}" for row in result])

    # Redirect back to the index page with results as a query parameter
    # This is a simple way to display results on the same page.
    from flask import redirect, url_for
    return redirect(url_for('index', wc_results=results_str))

@app.route('/generate_numbers', methods=['POST'])
def generate_numbers():
    try:
        num_rows = int(request.form.get('num_rows', 100000))
        if num_rows <= 0:
            return "Please enter a positive number of rows for generation.", 400 # Bad request
    except ValueError:
        return "Invalid input for number of rows. Please enter an integer.", 400

    spark = get_spark_session() # Get the existing or create a new SparkSession

    # Generate a large DataFrame with random numbers
    # Using range and then parallelizing is one way; for truly large,
    # distributed data generation, Spark has more advanced methods.
    data = [(i, float(i) * 0.1 + (i % 5)) for i in range(num_rows)]
    df = spark.createDataFrame(data, ["id", "value"])

    # Perform some aggregations using PySpark
    total_sum_val = df.agg(sum("value").alias("total_value")).collect()[0]["total_value"]
    # Calculate mean using the summary method for a column
    avg_value_df = df.select(col("value")).summary("mean")
    avg_value = float(avg_value_df.collect()[0]["value"]) # Extract the mean value

    results_str = f"Generated {num_rows} rows.\n" \
                  f"Total sum of values: {total_sum_val:.2f}\n" \
                  f"Average value: {avg_value:.2f}"

    # Redirect back to the index page with results as a query parameter
    from flask import redirect, url_for
    return redirect(url_for('index', rn_results=results_str))

# --- Application Entry Point ---

# This block MUST start at the very first column (no indentation)
if __name__ == '__main__':
    # When running with 'python app.py', Flask's development server starts.
    # For production, you would use a WSGI server like Gunicorn or uWSGI.
    app.run(debug=True, host='0.0.0.0', port=5000)