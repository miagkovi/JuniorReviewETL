import time
import psutil
import os
import logging

def get_logger(name):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler("etl_process.log"), logging.StreamHandler()]
    )
    return logging.getLogger(name)

def log_resource_usage(logger):
    """Logs the current resource usage (memory and CPU) of the process."""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info().rss / (1024 * 1024)  # Convert to MB
    cpu_usage = psutil.cpu_percent(interval=0.1)
    logger.info(f"Resource Usage: Memory: {mem_info:.2f} MB, CPU: {cpu_usage}%")