import requests
import time
from multiprocessing import Pool, cpu_count
from collections import Counter

# Configuration
BASE_URL = "http://72.60.221.150:8080"
STUDENT_ID = "MDS202528"  

def get_secret_key(student_id):
    """Helper to get the dynamic SHA256 secret key."""
    try:
        response = requests.post(f"{BASE_URL}/login", json={"student_id": student_id})
        response.raise_for_status()
        return response.json().get("secret_key")
    except Exception as e:
        print(f"Login failed: {e}")
        return None

def fetch_with_retry(filename, secret_key):
    """Fetches title with handling for 429 Throttling."""
    while True:
        payload = {"secret_key": secret_key, "filename": filename}
        response = requests.post(f"{BASE_URL}/lookup", json=payload)
        
        if response.status_code == 200:
            return response.json().get("title", "")
        elif response.status_code == 429:
            time.sleep(1)  # Wait for rate limit to reset
        else:
            return ""

def mapper(filename_chunk):
    """Map phase: processes a list of filenames and returns word counts."""
    key = get_secret_key(STUDENT_ID)
    counts = Counter()
    
    if not key:
        return counts

    for fname in filename_chunk:
        title = fetch_with_retry(fname, key)
        if title:
            # Get the first word, lowercase it for consistency
            first_word = title.strip().split()[0]
            counts[first_word] += 1
            
    return counts

def verify_top_10(student_id, top_10_list):
    """Verification phase: submits results to the server."""
    key = get_secret_key(student_id)
    if not key: return
    
    payload = {"secret_key": key, "top_10": top_10_list}
    response = requests.post(f"{BASE_URL}/verify", json=payload)
    print(f"Verification Result: {response.json()}")

if __name__ == "__main__":
    # 1. Prepare filenames and chunks
    all_files = [f"pub_{i}.txt" for i in range(1000)]
    num_workers = cpu_count()
    chunk_size = len(all_files) // num_workers
    chunks = [all_files[i:i + chunk_size] for i in range(0, len(all_files), chunk_size)]

    # 2. Map: Parallelize the retrieval
    print(f"Starting Map phase with {num_workers} workers...")
    with Pool(num_workers) as pool:
        list_of_counters = pool.map(mapper, chunks)

    # 3. Reduce: Combine results
    print("Starting Reduce phase...")
    final_counts = Counter()
    for c in list_of_counters:
        final_counts.update(c)

    # 4. Identify Top 10
    # most_common(10) returns [('word', count), ...]
    top_10_tuples = final_counts.most_common(10)
    top_10 = [word for word, count in top_10_tuples]
    
    print(f"Top 10 Words: {top_10}")

    # 5. Verify
    if top_10:
        verify_top_10(STUDENT_ID, top_10)
