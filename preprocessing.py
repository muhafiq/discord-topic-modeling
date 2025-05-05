from modules.utils import load_english_file
from modules.gcs import gcs_bucket
import json
import tempfile

# 1. Ambil daftar file JSON dari ID-ID yang bahasa Inggris
english_datasets = load_english_file()
english_filenames = set(f"{entry['id']}.json" for entry in english_datasets)

# 2. Stream per baris dari file besar tanpa loading penuh ke RAM
def stream_json_lines_from_large_blob(blob_path):
    blob = gcs_bucket.blob(blob_path)

    with tempfile.NamedTemporaryFile(mode="w+b", delete=True) as tmp_file:
        print(f"Downloading {blob_path} to {tmp_file.name}")
        blob.download_to_file(tmp_file)
        tmp_file.seek(0)

        for line in tmp_file:
            try:
                line = line.decode("utf-8").strip()
                if not line:
                    continue
                yield json.loads(line)
            except Exception:
                continue  # skip error lines

# 3. Stream semua file yang cocok dengan english_filenames
def load_english_dataset(prefix):
    blobs = gcs_bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        filename = blob.name.split("/")[-1]
        if filename in english_filenames:
            yield from stream_json_lines_from_large_blob(blob.name)

# 4. Contoh pemrosesan
for msg in load_english_dataset("datasets-v2/./"):
    content = msg.get("content", "")
    if content:
        print(content)
