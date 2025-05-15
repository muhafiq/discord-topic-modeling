from bertopic import BERTopic
import json
import tempfile
import os
from modules.gcs import gcs_bucket as bucket

# --- CONFIG ---
CLEANED_FOLDER = "cleaned"
MODEL_FOLDER = "model/bert"

# --- STREAM FROM GCS WITH OPTIONAL SAMPLING ---
def stream_documents_from_gcs(prefix, max_docs=None):
    total = 0
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        filename = blob.name.split("/")[-1]
        print(f"[LOADING] {filename}")
        for msg in stream_json_lines_from_large_blob(blob.name):
            if isinstance(msg, list):
                for line in msg:
                    if isinstance(line, str) and line.strip():
                        yield line
                        total += 1
                        if max_docs and total >= max_docs:
                            return

# --- STREAM FILE BY LINES ---
def stream_json_lines_from_large_blob(blob_path):
    blob = bucket.blob(blob_path)
    tmp_file = tempfile.NamedTemporaryFile(mode="w+b", delete=False)
    try:
        print(f"[DOWNLOADING] {blob_path} to {tmp_file.name}")
        blob.download_to_file(tmp_file)
        tmp_file.seek(0)

        for line in tmp_file:
            try:
                line = line.decode("utf-8").strip()
                if line:
                    yield json.loads(line)
            except Exception:
                continue
    finally:
        tmp_file.close()
        os.remove(tmp_file.name)

# --- TRAIN BERTopic ---
def train_bertopic_model_on_gcs_data(prefix, max_docs=None):
    print(f"[START] {'Sampling' if max_docs else 'Full'} BERTopic training...")
    all_cleaned_texts = list(stream_documents_from_gcs(prefix, max_docs=max_docs))
    print(f"[LOADED] {len(all_cleaned_texts)} documents ready for training")

    topic_model = BERTopic()
    topics, _ = topic_model.fit_transform(all_cleaned_texts)

    # --- Save model locally then upload to GCS ---
    local_model_path = "/tmp/bertopic_model"
    topic_model.save(local_model_path)

    model_filename = "topic_model_sample" if max_docs else "topic_model_full"
    blob = bucket.blob(f"{MODEL_FOLDER}/{model_filename}")
    blob.upload_from_filename(local_model_path)

    print(f"[SAVED] Model saved to GCS at {MODEL_FOLDER}/{model_filename}")

# --- RUN SCRIPT ---
if __name__ == "__main__":
    # train_bertopic_model_on_gcs_data(CLEANED_FOLDER, max_docs=50_000)  # For sampling
    # train_bertopic_model_on_gcs_data(CLEANED_FOLDER)  # For full dataset
    train_bertopic_model_on_gcs_data(CLEANED_FOLDER, max_docs=1_000_000)
