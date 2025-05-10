import json
from tempfile import NamedTemporaryFile
from modules.gcs import gcs_bucket
from tqdm import tqdm

# load english file
def load_english_file():
    blob = gcs_bucket.blob("additional/english.json")
    content = blob.download_as_text(encoding="utf-8")

    data = json.loads(content)
    return data;

def update_cleaned_status():
    # 1. Load english.json
    blob = gcs_bucket.blob("additional/english.json")
    english_data = json.loads(blob.download_as_text(encoding='utf-8'))
    
    # 2. List semua file yang sudah dibersihkan di GCS
    cleaned_blobs = list(gcs_bucket.list_blobs(prefix="cleaned/"))
    cleaned_files = {blob.name.split("/")[-1] for blob in cleaned_blobs if not blob.name.endswith('/')}
    
    # 3. Update status cleaned
    updated_count = 0
    for item in tqdm(english_data, desc="Updating cleaned status"):
        file_pattern = f"{item['id']}-"
        is_cleaned = any(file.startswith(file_pattern) for file in cleaned_files)
        
        if 'cleaned' not in item or item['cleaned'] != is_cleaned:
            item['cleaned'] = is_cleaned
            updated_count += 1
    
    # 4. Simpan kembali ke GCS - SOLUSI FIX
    blob.upload_from_string(
        json.dumps(english_data, ensure_ascii=False),
        content_type='application/json; charset=utf-8'
    )
    
    print(f"\nUpdated {updated_count} records")
    print(f"Total cleaned files: {sum(item.get('cleaned', False) for item in english_data)}/{len(english_data)}")

def reset_english_file_cleaning_status():
    # 1. Load english.json dari GCS
    blob = gcs_bucket.blob("additional/english.json")
    english_data = json.loads(blob.download_as_text(encoding="utf-8"))

    # 2. Set cleaned = False untuk semua entri
    for item in english_data:
        item['cleaned'] = False

    # 3. Upload kembali ke GCS
    blob.upload_from_string(
        json.dumps(english_data, indent=2, ensure_ascii=False),
        content_type="application/json; charset=utf-8"
    )

    print(f"Reset {len(english_data)} records: cleaned = False")
