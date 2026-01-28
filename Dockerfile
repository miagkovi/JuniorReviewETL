# Oficial Bitnami Spark image as base
FROM bitnami/spark:3.5.0

USER root
WORKDIR /app

# Create output directory with appropriate permissions
RUN mkdir -p /app/output && chmod -R 777 /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ETL script into the container
COPY etl_script.py .

# Set the default command to run the ETL script
CMD ["spark-submit", "etl_script.py"]