# Base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Spark Streaming script
COPY src/ .

# Command to run Spark job
CMD ["python3", "main.py"]