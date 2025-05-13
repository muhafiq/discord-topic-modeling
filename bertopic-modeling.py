from bertopic import BERTopic
import json
import tempfile
import os
from modules.gcs import gcs_bucket as bucket

# --- CONFIG ---
CLEANED_FOLDER = "cleaned"
MODEL_FOLDER = "model"
BATCH_SIZE = 500  # Batch size for processing

# --- STREAM FROM GCS FILE ---
def stream_json_lines_from_large_blob(blob_path):
    # Function to stream the JSON file from GCS
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

# --- TOPIC MODELING ---
def train_bertopic_model_on_gcs_data(prefix):
    topic_model = BERTopic()
    all_cleaned_texts = []

    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        filename = blob.name.split("/")[-1]
        print(f"[PROCESSING] {filename}")
        for msg in stream_json_lines_from_large_blob(blob.name):
            # Assuming msg is already an array of strings
            texts = msg  # msg is the list of strings directly
            if texts:
                all_cleaned_texts.extend(texts)

            # Process in batches to avoid memory overload
            if len(all_cleaned_texts) >= BATCH_SIZE:
                print(f"[TRAINING] Training BERTopic model with {len(all_cleaned_texts)} documents (batch)")
                topics, _ = topic_model.fit_transform(all_cleaned_texts)
                all_cleaned_texts = []  # Reset the list after training

    # Final training with remaining texts
    if all_cleaned_texts:
        print(f"[TRAINING] Training BERTopic model with {len(all_cleaned_texts)} remaining documents")
        topics, _ = topic_model.fit_transform(all_cleaned_texts)

    # --- Save the model to GCS ---
    model_filename = "topic_model"
    local_model_path = "/tmp/bertopic_model"
    topic_model.save(local_model_path)

    # Upload the model to GCS
    blob = bucket.blob(f"{MODEL_FOLDER}/{model_filename}")
    blob.upload_from_filename(local_model_path)

    print(f"[SAVED] Model saved to {MODEL_FOLDER}/{model_filename} on GCS")

# --- RUN ---
if __name__ == "__main__":
    # Specify the prefix where your cleaned data is stored (e.g., "cleaned/")
    train_bertopic_model_on_gcs_data(CLEANED_FOLDER)
