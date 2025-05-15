import json
import os
import shutil
from tqdm import tqdm
from gensim.corpora import Dictionary, MmCorpus
from gensim.models import LdaModel
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from modules.gcs import gcs_bucket

# === Preprocessing ===
def preprocess(doc):
    return [token for token in simple_preprocess(doc) if token not in STOPWORDS and len(token) > 2]

# === Stream documents from GCS ===
def stream_documents_from_gcs(max_docs=None):
    total = 0
    for blob in tqdm(gcs_bucket.list_blobs(prefix="cleaned/"), desc="Streaming GCS"):
        if blob.name.endswith('/'):
            continue
        try:
            content = blob.download_as_text()
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

# === Save model and dictionary properly ===
def save_gensim_model_to_gcs(lda_model, dictionary, gcs_prefix="model/lda_gensim"):
    model_dir = "model/lda_gensim"  # folder permanen di OS sekarang
    os.makedirs(model_dir, exist_ok=True)

    # Save gensim model (creates multiple files)
    lda_model.save(f"{model_dir}/lda.model")
    dictionary.save(f"{model_dir}/dictionary.dict")

    # Zip the model directory (opsional)
    zip_path = "model/lda_model.zip"
    shutil.make_archive(zip_path.replace(".zip", ""), 'zip', model_dir)

    # Upload zip dan dictionary ke GCS
    gcs_bucket.blob(f"{gcs_prefix}/lda_model.zip").upload_from_filename(zip_path)
    gcs_bucket.blob(f"{gcs_prefix}/dictionary.dict").upload_from_filename(f"{model_dir}/dictionary.dict")

    print(f"[✅] Model dan dictionary berhasil disimpan ke GCS: gs://{gcs_prefix}/")

# === MAIN SCRIPT ===
if __name__ == "__main__":
    print("📖 Membuat dictionary (100 juta dokumen)...")
    dictionary = Dictionary(stream_documents_from_gcs(max_docs=100_000_000))
    dictionary.filter_extremes(no_below=5, no_above=0.95)

    corpus_path = "corpus.mm"
    print("💾 Menyimpan corpus ke disk...")
    MmCorpus.serialize(corpus_path, (dictionary.doc2bow(doc) for doc in stream_documents_from_gcs()))

    print("🔁 Training model Gensim LDA...")
    corpus = MmCorpus(corpus_path)
    lda = LdaModel(
        corpus=corpus,
        id2word=dictionary,
        num_topics=20,
        chunksize=2000,
        passes=1,
        iterations=50,
        eval_every=None,
        update_every=1,
        random_state=42
    )

    print("\n=== Topik yang Ditemukan ===")
    for i, topic in lda.show_topics(num_topics=20, num_words=10, formatted=False):
        print(f"Topik #{i+1}: " + " | ".join([word for word, _ in topic]))
        print()

    print("💾 Menyimpan model dan dictionary ke GCS...")
    save_gensim_model_to_gcs(lda, dictionary)

    os.remove(corpus_path)
