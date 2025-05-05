from modules.utils import load_english_file
from modules.gcs import gcs_bucket
import json
import tempfile
import os
import string
from datetime import datetime
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from num2words import num2words
import nltk
import re

# --- INIT / DOWNLOAD ---
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')
    nltk.download('punkt_tab')

# --- CONFIG ---
english_datasets = load_english_file()
english_filenames = set(f"{entry['id']}.json" for entry in english_datasets)
EN_STOPWORDS = set(stopwords.words("english"))
PUNCTUATION_TABLE = str.maketrans('', '', string.punctuation)

# --- HELPERS ---

def is_possible_user_mention(token):
    # ID numeric panjang atau ID hex: biasanya tag user
    return (token.isdigit() and len(token) >= 17) or re.fullmatch(r'[a-f0-9]{6,}', token)

def is_username_with_id(token):
    # Misal: ghost123456789012345678
    return bool(re.fullmatch(r'[a-zA-Z]+[0-9]{6,}', token))

def is_non_latin(token):
    # Hilangkan kata dengan karakter non-Latin (CJK, Cyrillic, dll)
    return any(ord(c) > 127 for c in token)

def is_repetitive(text):
    return bool(re.search(r'(.{3,}?)\1{3,}', text))

def clean_message(msg):
    # Drop jika berisi spam string atau pengulangan ekstrem
    if 'vaelonchie' in msg or is_repetitive(msg):
        return None
    # Drop jika terlalu pendek atau tidak informatif
    if len(msg.strip()) < 10:
        return None
    return msg

# --- MAIN CLEANING FUNCTION ---

def clean_text(text):
    # Bersihkan dari unicode
    text = re.sub(r'\x1b\[[0-9;]*m', '', text)

    # Bersihkan pesan secara keseluruhan terlebih dahulu
    cleaned_msg = clean_message(text)
    if cleaned_msg is None:
        return []  # Pesan dibuang jika tidak memenuhi kriteria

    # Proses pembersihan lebih lanjut pada teks
    text = cleaned_msg.lower()
    text = text.translate(PUNCTUATION_TABLE)

    # Hapus link (termasuk obfuscated)
    text = re.sub(
        r'\b(?:https?|httpx|httpsx|cdn|media|tenor|discord|twitter|fixup|attachments|xcom|net|com|status)[^\s]*',
        '',
        text
    )

    # Jika setelah penghapusan kosong, return []
    if not text.strip():
        return []

    # Tokenisasi
    tokens = word_tokenize(text)

    # Normalisasi angka
    def normalize_number(w):
        if w.isdigit():
            try:
                return num2words(int(w))
            except Exception:
                return w
        return w

    tokens = [normalize_number(w) for w in tokens]

    # Filter stopwords, tag user, username+id, kata non-latin, dan kosong
    cleaned = [
        w for w in tokens
        if w not in EN_STOPWORDS
        and not is_possible_user_mention(w)
        and not is_username_with_id(w)
        and not is_non_latin(w)
        and w.strip() != ""
    ]

    return cleaned

# --- YEAR EXTRACTOR ---
def extract_year(timestamp_str):
    try:
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        return str(dt.year)
    except Exception:
        return "unknown"

# --- GCS UPLOAD ---
def write_cleaned_texts_to_gcs(file_id, year, texts):
    blob_name = f"cleaned/{file_id}-{year}.json"
    blob = gcs_bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(texts), content_type="application/json")
    print(f"[SAVED] {blob_name} ({len(texts)} entries)")

# --- STREAM FROM GCS FILE ---
def stream_json_lines_from_large_blob(blob_path):
    blob = gcs_bucket.blob(blob_path)
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

# --- MAIN PIPELINE ---
def process_and_save_cleaned_data(prefix):
    blobs = gcs_bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        filename = blob.name.split("/")[-1]
        if filename not in english_filenames:
            continue

        file_id = filename.replace(".json", "")
        print(f"[PROCESSING] {filename}")
        grouped_by_year = {}
        total = 0

        for msg in stream_json_lines_from_large_blob(blob.name):
            if msg.get("is_bot", False):
                continue
            content = msg.get("content", "")
            if not content:
                continue

            words = clean_text(content)
            if not words:
                continue

            year = extract_year(msg.get("timestamp", ""))
            grouped_by_year.setdefault(year, []).append(" ".join(words))
            total += 1

        for year, texts in grouped_by_year.items():
            write_cleaned_texts_to_gcs(file_id, year, texts)

        print(f"[DONE] {filename} - {total} valid chats")

# --- RUN ---
if __name__ == "__main__":
    process_and_save_cleaned_data("datasets-v2/./")
