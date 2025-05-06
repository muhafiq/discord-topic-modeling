from google.cloud import storage
import os
from dotenv import load_dotenv
import joblib
from tempfile import NamedTemporaryFile

load_dotenv()

BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', '')
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__));

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(CURRENT_PATH, '..', 'gcs-credentials.json')

storage_client = storage.Client();
gcs_bucket = storage_client.bucket(BUCKET_NAME)

def save_model_to_gcs(model, vectorizer, folder="models"):
    
    # Save LDA model
    with NamedTemporaryFile(suffix='.joblib') as tmp:
        joblib.dump(model, tmp.name)
        blob = gcs_bucket.blob(f"{folder}/lda_model.joblib")
        blob.upload_from_filename(tmp.name)
        print(f"Saved LDA model to gs://{BUCKET_NAME}/{folder}/lda_model.joblib")
    
    # Save Vectorizer
    with NamedTemporaryFile(suffix='.joblib') as tmp:
        joblib.dump(vectorizer, tmp.name)
        blob = gcs_bucket.blob(f"{folder}/vectorizer.joblib")
        blob.upload_from_filename(tmp.name)
        print(f"Saved vectorizer to gs://{BUCKET_NAME}/{folder}/vectorizer.joblib")

def load_model_from_gcs(folder="models"):
    
    # Load LDA model
    blob = gcs_bucket.blob(f"{folder}/lda_model.joblib")
    with NamedTemporaryFile() as tmp:
        blob.download_to_filename(tmp.name)
        lda = joblib.load(tmp.name)
    
    # Load Vectorizer
    blob = gcs_bucket.blob(f"{folder}/vectorizer.joblib")
    with NamedTemporaryFile() as tmp:
        blob.download_to_filename(tmp.name)
        vectorizer = joblib.load(tmp.name)
    
    return lda, vectorizer