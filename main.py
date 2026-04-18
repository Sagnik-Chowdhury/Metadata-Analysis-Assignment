import requests
import time
from multiprocessing import Pool, cpu_count

# Configuration
# The server's public IP address
BASE_URL = "http://72.60.221.150:8080"
STUDENT_ID = "MDS202528"  # Replace with your actual student ID

def mapper(filename_chunk):
    """
    Map phase: Takes a chunk of filenames and returns first word frequencies.
    Handles login and throttling within the worker.
    """
    # 1. Authentication: Get the unique Secret Key for this worker session 
    key = get_secret_key(STUDENT_ID)
    counts = Counter()
    
    if not key:
        return counts

    # 2. Retrieval: Loop through the chunk and fetch titles 
    for fname in filename_chunk:
        # fetch_with_retry handles the 429 Throttling logic 
        title = fetch_with_retry(fname, key)
        
        if title:
            # Get the first word of the title 
            first_word = title.strip().split()[0]
            counts[first_word] += 1
            
    return counts

def get_publication_title(student_id, filename):
    """
    Implementation for Step 1 and Step 2 of the RPC process.
    """
    # 1. Log in to get the dynamic SHA256 secret key 
    secret_key = get_secret_key(student_id)
    if not secret_key:
        return ""

    # 2. Use the key to retrieve the publication title
    # 3. Handle 429 (Too Many Requests) with a retry mechanism 
    title = fetch_with_retry(filename, secret_key)
    
    # 4. Return the title (fetch_with_retry already handles 404/500 by returning "")
    return title

def verify_top_10(student_id, top_10_list):
    """
    Final step: Authenticates and submits the Top 10 list for grading.
    """
    # 1. Log in to get the dynamic SHA256 secret key 
    key = get_secret_key(student_id)
    if not key:
        print("Verification failed: Could not obtain secret key.")
        return

    # 2. Submit the top_10_list to the /verify endpoint 
    payload = {
        "secret_key": key,
        "top_10": top_10_list
    }
    
    try:
        response = requests.post(f"{BASE_URL}/verify", json=payload)
        response.raise_for_status()
        
        # 3. Print the final score and message from the server 
        result = response.json()
        print(f"Verification Result: {result}")
    except Exception as e:
        print(f"Error during verification: {e}")

if __name__ == "__main__":
    # 1. Divide filenames (pub_0.txt to pub_999.txt) into chunks 
    all_files = [f"pub_{i}.txt" for i in range(1000)]
    num_workers = cpu_count()
    chunk_size = len(all_files) // num_workers
    chunks = [all_files[i:i + chunk_size] for i in range(0, len(all_files), chunk_size)]

    # 2. Use multiprocessing.Pool to map your 'mapper' function over the chunks 
    print(f"Starting Map phase with {num_workers} workers...")
    with Pool(num_workers) as pool:
        list_of_counters = pool.map(mapper, chunks)

    # 3. Combine (Reduce) the frequency counts from all workers 
    print("Starting Reduce phase...")
    final_counts = Counter()
    for c in list_of_counters:
        final_counts.update(c)

    # 4. Identify the Top 10 most frequent first words 
    # most_common(10) returns a list of (word, count) tuples
    top_10_tuples = final_counts.most_common(10)
    top_10 = [word for word, count in top_10_tuples]
    
    print(f"Top 10 Words found: {top_10}")

    # 5. Call verify_top_10(STUDENT_ID, top_10) 
    if top_10:
        verify_top_10(STUDENT_ID, top_10)
    else:
        print("Compute the top 10 words first!")
