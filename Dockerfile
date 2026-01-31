# Using the official Apache Spark image as base
FROM apache/spark:3.5.0

USER root

# Set up necessary dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create necessary directories with appropriate permissions
RUN mkdir -p /app/src /app/tests /app/output && chmod -R 777 /app

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy source code and tests into the container
COPY src/ /app/src/
COPY tests/ /app/tests/

# Set PYTHONPATH to include the application directory
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Test and run the application
CMD pytest tests/ && /opt/spark/bin/spark-submit --master local[*] src/app.py