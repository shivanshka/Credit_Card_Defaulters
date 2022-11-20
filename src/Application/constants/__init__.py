from datetime import datetime
import os

ROOT_DIR = os.getcwd()
CURRENT_TIMESTAMP = f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"

LOG_DIR = "Running_logs"
ARTIFACTS_DIR = "artifacts"