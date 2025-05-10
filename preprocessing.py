from modules.utils import load_english_file, update_cleaned_status
from modules.gcs import gcs_bucket
import json
import tempfile
import os
import string
from datetime import datetime
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords, words
from nltk.stem import WordNetLemmatizer
from num2words import num2words
import nltk
import re
from langdetect import detect

# --- INIT / DOWNLOAD ---
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

try:
    nltk.data.find('corpora/words')
except LookupError:
    nltk.download('words')

try:
    nltk.data.find('corpora/wordnet')
except LookupError:
    nltk.download('wordnet')

nltk.download('punkt_tab')

# --- CONFIG ---
english_datasets = load_english_file()
english_filenames = set(f"{entry['id']}.json" for entry in english_datasets)
EN_STOPWORDS = set(stopwords.words("english"))
ENGLISH_WORDS = set(w.lower() for w in words.words())
PUNCTUATION_TABLE = str.maketrans('', '', string.punctuation)
LEMMATIZER = WordNetLemmatizer()

# --- HELPERS ---
def is_possible_user_mention(token):
    return (token.isdigit() and len(token) >= 17) or re.fullmatch(r'[a-f0-9]{6,}', token)

def is_username_with_id(token):
    return bool(re.search(r'[a-zA-Z]', token) and re.search(r'\d{6,}', token))

def is_non_latin(token):
    return any(ord(c) > 127 for c in token)

def is_repetitive(text):
    return bool(re.search(r'(.{3,}?)\1{3,}', text))

def clean_message(msg):
    if 'vaelonchie' in msg or is_repetitive(msg):
        return None
    if len(msg.strip()) < 10:
        return None
    return msg

def is_random_alphanumeric(token):
    return (
        len(token) >= 10 and
        any(c.isalpha() for c in token) and
        any(c.isdigit() for c in token)
    )

def is_unknown_word(w):
    return re.fullmatch(r'unknown_\d+', w) is not None

def is_english_langdetect(text):
    try:
        return detect(text) == 'en'
    except:
        return False

def english_word_ratio(tokens):
    if not tokens:
        return 0.0
    english_count = sum(1 for t in tokens if t.lower() in ENGLISH_WORDS)
    return english_count / len(tokens)

def is_clean_english_sentence(text, min_ratio=0.6):
    if not is_english_langdetect(text):
        return False
    tokens = word_tokenize(text.lower())
    return english_word_ratio(tokens) >= min_ratio

# --- MAIN CLEANING FUNCTION ---
def clean_text(text):
    text = re.sub(r'\x1b\[[0-9;]*m', '', text)
    cleaned_msg = clean_message(text)
    if cleaned_msg is None:
        return []

    if not is_clean_english_sentence(cleaned_msg):
        return []

    text = cleaned_msg.lower()
    text = re.sub(r"\b(?:\w+n't|'re|'s|'d|'ll|'ve|'m)\b", lambda m: {
        "n't": "not", "'re": "are", "'s": "is", "'d": "would",
        "'ll": "will", "'ve": "have", "'m": "am"
    }.get(m.group(0), m.group(0)), text)

    text = text.translate(PUNCTUATION_TABLE)
    text = re.sub(r'\b(?:https?|cdn|media|tenor|discord|twitter|attachments)[^\s]*', '', text)
    if not text.strip():
        return []

    tokens = word_tokenize(text)

    def normalize_number(w):
        if w.isdigit() and len(w) <= 6:
            try:
                return num2words(int(w))
            except Exception:
                return w
        return w

    tokens = [normalize_number(w) for w in tokens]
    tokens = [LEMMATIZER.lemmatize(w) for w in tokens]

    cleaned = [
        w for w in tokens
        if w not in EN_STOPWORDS
        and not is_possible_user_mention(w)
        and not is_username_with_id(w)
        and not is_random_alphanumeric(w)
        and not is_unknown_word(w)
        and not is_non_latin(w)
        and w.strip() != ""
        and len(w) <= 20
    ]

    if len(cleaned) < 3:
        return []

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
    blob_name = f"cleaned-v2/{file_id}-{year}.json"
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
    english_data = load_english_file()
    english_data_map = {entry['id']: entry for entry in english_data}

    blobs = gcs_bucket.list_blobs(prefix=prefix)
    processed_files = set()

    for blob in blobs:
        filename = blob.name.split("/")[-1]
        file_id = filename.replace(".json", "")

        if filename not in english_filenames:
            continue
        if english_data_map.get(file_id, {}).get('cleaned', False):
            print(f"[SKIPPING] {filename} (already cleaned)")
            continue

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

        for entry in english_data:
            if entry['id'] == file_id:
                entry['cleaned'] = True
                break

        update_english_json(english_data)
        processed_files.add(file_id)

        print(f"[DONE] {filename} - {total} valid chats")

def update_english_json(data):
    blob = gcs_bucket.blob("additional/english.json")
    blob.upload_from_string(
        json.dumps(data, indent=2, ensure_ascii=False),
        content_type="application/json"
    )
    print("[UPDATED] english.json with new cleaned statuses")

# --- RUN ---
if __name__ == "__main__":
    # update_cleaned_status()
    process_and_save_cleaned_data("datasets-v2/./")
