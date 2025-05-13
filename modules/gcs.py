from google.cloud import storage
import os
from dotenv import load_dotenv
from tempfile import NamedTemporaryFile

load_dotenv()

BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', '')
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__));

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(CURRENT_PATH, '..', 'gcs-credentials.json')

storage_client = storage.Client();
gcs_bucket = storage_client.bucket(BUCKET_NAME)

def save_model_to_gcs(lda_model, dictionary, folder="models"):
    # Save LDA model
    with NamedTemporaryFile(suffix='.model') as tmp:
        lda_model.save(tmp.name)
        blob = gcs_bucket.blob(f"{folder}/lda_gensim.model")
        blob.upload_from_filename(tmp.name)
        print(f"Saved Gensim LDA model to gs://{gcs_bucket.name}/{folder}/lda_gensim.model")

    # Save dictionary
    with NamedTemporaryFile(suffix='.dict') as tmp:
        dictionary.save(tmp.name)
        blob = gcs_bucket.blob(f"{folder}/dictionary.dict")
        blob.upload_from_filename(tmp.name)
        print(f"Saved dictionary to gs://{gcs_bucket.name}/{folder}/dictionary.dict")


def load_model_from_gcs(folder="models"):
    # Load LDA model
    from gensim.models import LdaModel
    from gensim.corpora import Dictionary

    with NamedTemporaryFile(suffix='.model') as tmp:
        blob = gcs_bucket.blob(f"{folder}/lda_gensim.model")
        blob.download_to_filename(tmp.name)
        lda_model = LdaModel.load(tmp.name)

    # Load dictionary
    with NamedTemporaryFile(suffix='.dict') as tmp:
        blob = gcs_bucket.blob(f"{folder}/dictionary.dict")
        blob.download_to_filename(tmp.name)
        dictionary = Dictionary.load(tmp.name)

    return lda_model, dictionary