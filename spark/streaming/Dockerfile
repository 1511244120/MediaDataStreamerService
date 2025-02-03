FROM bitnami/spark:3.5.4

# Set the working directory
WORKDIR /app

# Install required libraries
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ .

# Set environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV PYSPARK_PYTHON=python3

# Run the application
CMD [ "spark-submit", \
"--master", "spark://spark-master:7077", \
"--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
"main.py" ]