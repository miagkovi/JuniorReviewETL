import os
import kagglehub
from utils import get_logger

logger = get_logger(__name__)

class DatasetDownloader:
    def __init__(self, dataset_id="ylenialongo/pizza-sales"):
        self.dataset_id = dataset_id

    def fetch_data_path(self) -> str:
        """Fetches the dataset and returns the local path."""
        try:
            logger.info(f"Downloading dataset: {self.dataset_id}")
            root_path = kagglehub.dataset_download(self.dataset_id)
            
            # Check for specific subdirectory
            target_path = os.path.join(root_path, "pizza_sales")
            final_path = target_path if os.path.exists(target_path) else root_path
            
            logger.info(f"Data is ready at: {final_path}")
            return final_path
        except Exception as e:
            logger.error(f"Failed to download dataset: {e}")
            raise