# Oficial Bitnami Spark image as base
FROM apache/spark:3.5.0

USER root
RUN apt-get update && apt-get install -y python3-pip
WORKDIR /app

# Create output directory with appropriate permissions
RUN mkdir -p /app/output && chmod -R 777 /app

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy ETL script into the container
COPY etl_script.py .

# Set the default command to run the ETL script
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "etl_script.py"]