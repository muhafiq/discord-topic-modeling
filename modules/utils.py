import json
from modules.gcs import gcs_bucket

# load english file
def load_english_file():
    blob = gcs_bucket.blob("additional/english.json")
    content = blob.download_as_text(encoding="utf-8")

    data = json.loads(content)
    return data;