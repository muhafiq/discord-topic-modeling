import json
from tqdm import tqdm
from modules.gcs import gcs_bucket, save_model_to_gcs
from gensim.corpora import Dictionary
from gensim.models import LdaModel
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS

# Optional: custom preprocessing
def preprocess(doc):
    return [token for token in simple_preprocess(doc) if token not in STOPWORDS and len(token) > 2]

# Stream documents from GCS
def stream_documents_from_gcs(max_docs=None):
    total = 0
    blobs = gcs_bucket.list_blobs(prefix="cleaned/")
    
    for blob in tqdm(list(blobs), desc="Streaming GCS"):
        if blob.name.endswith('/'):
            continue

        content = blob.download_as_text()
        try:
            data = json.loads(content)
            if isinstance(data, list):
                for doc in data:
                    if isinstance(doc, str) and doc.strip():
                        yield preprocess(doc)
                        total += 1
                        if max_docs and total >= max_docs:
                            return
        except json.JSONDecodeError:
            continue

# Build dictionary (pass 1)
print("📖 Membuat dictionary...")
dictionary = Dictionary(stream_documents_from_gcs(max_docs=100000))  # Boleh semua kalau kuat

# Filter terms (optional)
dictionary.filter_extremes(no_below=5, no_above=0.95)

# Stream again for training (pass 2)
print("🔁 Training model Gensim LDA...")
corpus_stream = (dictionary.doc2bow(doc) for doc in stream_documents_from_gcs())

# Buat model LDA
lda = LdaModel(
    corpus=corpus_stream,
    id2word=dictionary,
    num_topics=20,
    chunksize=2000,
    passes=1,
    iterations=50,
    eval_every=None,
    update_every=1,
    random_state=42
)

# Cetak topik
print("\n=== Topik yang Ditemukan ===")
for i, topic in lda.show_topics(num_topics=20, num_words=10, formatted=False):
    print(f"Topik #{i+1}: " + " | ".join([word for word, _ in topic]))
    print()

# Simpan model dan dictionary ke GCS
save_model_to_gcs(lda, dictionary)
