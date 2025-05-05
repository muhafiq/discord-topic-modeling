from google.cloud import storage
import os
from dotenv import load_dotenv

load_dotenv()

CURRENT_PATH = os.path.dirname(os.path.abspath(__file__));

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(CURRENT_PATH, '..', 'gcs-credentials.json')

storage_client = storage.Client();
gcs_bucket = storage_client.bucket(os.environ.get('GCS_BUCKET_NAME', ''))