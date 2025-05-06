import collections
import json
from tqdm import tqdm
from modules.gcs import gcs_bucket, save_model_to_gcs

from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.decomposition import LatentDirichletAllocation

# Streaming vectorizer
vectorizer = HashingVectorizer(
    stop_words='english',
    alternate_sign=False,
    n_features=2**18
)

lda = LatentDirichletAllocation(
    n_components=20,
    learning_method='online',
    batch_size=1024,
    evaluate_every=-1,
    random_state=42
)

def stream_documents_from_gcs(batch_size=5000):
    """Stream documents from GCS cleaned folder"""
    batch = []
    # List all blobs in the cleaned folder
    blobs = gcs_bucket.list_blobs(prefix="cleaned/")
    
    for blob in tqdm(list(blobs)):
        # Skip directories
        if blob.name.endswith('/'):
            continue
            
        # Download and process each file
        content = blob.download_as_text()
        try:
            data = json.loads(content)
            if isinstance(data, list):
                for doc in data:
                    if isinstance(doc, str) and doc.strip():
                        batch.append(doc)
                        if len(batch) >= batch_size:
                            yield batch
                            batch = []
        except json.JSONDecodeError:
            print(f"Error decoding JSON from {blob.name}")
            continue
            
    if batch:
        yield batch

# Training loop with progress tracking
total_docs = 0
for batch in stream_documents_from_gcs(batch_size=5000):
    X = vectorizer.transform(batch)
    lda.partial_fit(X)
    total_docs += len(batch)
    print(f"Processed {total_docs} documents so far...")

def print_topics(model, vectorizer, n_top_words=10, n_sample_docs=1000):
    """Improved topic printing with actual word mapping"""
    print("\n=== Topik yang Ditemukan ===")
    
    # vocabulary mapping dari sample dokumen
    hash_to_words = collections.defaultdict(set)
    sample_count = 0
    
    # sample dokumen dari gcs untuk mapping
    blobs = list(gcs_bucket.list_blobs(prefix="cleaned/"))
    for blob in tqdm(blobs[:n_sample_docs], desc="Mapping hash to words"):
        content = blob.download_as_text()
        try:
            docs = json.loads(content)
            for doc in docs:
                if isinstance(doc, str):
                    words = doc.split()
                    for word in words:
                        hash_idx = vectorizer.transform([word]).nonzero()[1]
                        if hash_idx.size > 0:
                            hash_to_words[hash_idx[0]].add(word)
                    sample_count += 1
                    if sample_count >= n_sample_docs:
                        break
        except json.JSONDecodeError:
            continue
    
    # topik dengan kata aktual
    for topic_idx, topic in enumerate(model.components_):
        top_indices = topic.argsort()[:-n_top_words - 1:-1]
        
        # kata-kata untuk indeks teratas
        topic_words = []
        for idx in top_indices:
            words = list(hash_to_words.get(idx, set()))
            if words:
                # kata terpendek sebagai representasi (biasanya lebih meaningful)
                representative_word = min(words, key=len)
                topic_words.append(representative_word)
            else:
                topic_words.append(f"unknown_{idx}")
        
        # filter duplicate words
        seen = set()
        unique_words = [w for w in topic_words if not (w in seen or seen.add(w))]
        
        print(f"Topik #{topic_idx + 1}:")
        print(" | ".join(unique_words[:n_top_words]))
        print()

print_topics(lda, vectorizer)
save_model_to_gcs(lda, vectorizer)