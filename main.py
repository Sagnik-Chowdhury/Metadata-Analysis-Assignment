import requests
import time
from multiprocessing import Pool, cpu_count
from collections import Counter

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
BASE_URL   = "http://72.60.221.150:8080"
STUDENT_ID = "MDS202528"          # ← your student ID

MAX_RETRIES  = 5      # max attempts before giving up on a file
RETRY_SLEEP  = 1.0    # seconds to wait after a 429
ERROR_SLEEP  = 0.5    # seconds to wait after a generic error


# ──────────────────────────────────────────────
# Helper: login and get a fresh secret key
# ──────────────────────────────────────────────
def get_secret_key(student_id: str) -> str | None:
    """POST /login and return the 64-char SHA-256 secret key."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                f"{BASE_URL}/login",
                json={"student_id": student_id},
                timeout=10
            )
            if resp.status_code == 200:
                return resp.json().get("secret_key")
            elif resp.status_code == 429:
                print(f"[login] 429 throttled – waiting {RETRY_SLEEP}s")
                time.sleep(RETRY_SLEEP)
            else:
                print(f"[login] Unexpected status {resp.status_code}")
                return None
        except Exception as e:
            print(f"[login] Error on attempt {attempt + 1}: {e}")
            time.sleep(ERROR_SLEEP)
    return None


# ──────────────────────────────────────────────
# Helper: fetch one publication title (with retries)
# ──────────────────────────────────────────────
def fetch_title(filename: str, student_id: str) -> str:
    """
    POST /lookup for a single file.
    Re-authenticates on every call so a stale key never causes
    silent data loss across a long-running chunk.
    Returns the title string, or "" on unrecoverable failure.
    """
    for attempt in range(MAX_RETRIES):
        # Get a fresh key each attempt – cheap and safe
        secret_key = get_secret_key(student_id)
        if not secret_key:
            print(f"[{filename}] Could not obtain secret key – retrying...")
            time.sleep(ERROR_SLEEP)
            continue

        try:
            resp = requests.post(
                f"{BASE_URL}/lookup",
                json={"secret_key": secret_key, "filename": filename},
                timeout=10
            )

            if resp.status_code == 200:
                return resp.json().get("title", "")

            elif resp.status_code == 429:
                # Server-side throttle: back off and retry
                print(f"[{filename}] 429 throttled – waiting {RETRY_SLEEP}s")
                time.sleep(RETRY_SLEEP)

            elif resp.status_code == 404:
                print(f"[{filename}] 404 Not Found – skipping")
                return ""

            else:
                print(f"[{filename}] Status {resp.status_code} – skipping")
                return ""

        except requests.exceptions.Timeout:
            print(f"[{filename}] Timeout on attempt {attempt + 1}")
            time.sleep(ERROR_SLEEP)

        except Exception as e:
            print(f"[{filename}] Error on attempt {attempt + 1}: {e}")
            time.sleep(ERROR_SLEEP)

    print(f"[{filename}] Giving up after {MAX_RETRIES} attempts")
    return ""


# ──────────────────────────────────────────────
# MAP phase
# ──────────────────────────────────────────────
def mapper(filename_chunk: list[str]) -> Counter:
    """
    Processes one chunk of filenames.
    Returns a Counter of {first_word: count} for that chunk.
    """
    counts = Counter()
    for fname in filename_chunk:
        title = fetch_title(fname, STUDENT_ID)
        if title and title.strip():
            first_word = title.strip().split()[0]
            counts[first_word] += 1
    return counts


# ──────────────────────────────────────────────
# VERIFY phase
# ──────────────────────────────────────────────
def verify_top_10(student_id: str, top_10_list: list[str]) -> None:
    """POST /verify and print the server's score + message."""
    secret_key = get_secret_key(student_id)
    if not secret_key:
        print("Could not obtain secret key for verification.")
        return

    try:
        resp = requests.post(
            f"{BASE_URL}/verify",
            json={"secret_key": secret_key, "top_10": top_10_list},
            timeout=10
        )
        result = resp.json()
        print("\n=== Verification Result ===")
        print(f"Score   : {result.get('score')} / {result.get('total')}")
        print(f"Correct : {result.get('correct')}")
        print(f"Message : {result.get('message')}")
    except Exception as e:
        print(f"Verification request failed: {e}")


# ──────────────────────────────────────────────
# MAIN – orchestrates Map → Reduce → Verify
# ──────────────────────────────────────────────
if __name__ == "__main__":

    # ── 1. Build the full file list ──────────────
    all_files = [f"pub_{i}.txt" for i in range(1000)]
    print(f"Total files to process: {len(all_files)}")

    # ── 2. Chunk the list evenly across workers ──
    num_workers = cpu_count()
    print(f"Workers (CPU cores): {num_workers}")

    # Use list comprehension to create equal chunks;
    # the last chunk absorbs any remainder automatically.
    chunk_size = max(1, len(all_files) // num_workers)
    chunks = [
        all_files[i : i + chunk_size]
        for i in range(0, len(all_files), chunk_size)
    ]
    print(f"Chunks created: {len(chunks)} (≈ {chunk_size} files each)\n")

    # ── 3. MAP – parallel title retrieval ────────
    print("Starting MAP phase...")
    start = time.time()

    with Pool(processes=num_workers) as pool:
        list_of_counters = pool.map(mapper, chunks)

    elapsed = time.time() - start
    print(f"MAP phase complete in {elapsed:.1f}s\n")

    # ── 4. REDUCE – merge all counters ───────────
    print("Starting REDUCE phase...")
    final_counts = Counter()
    for counter in list_of_counters:
        final_counts.update(counter)

    total_titles = sum(final_counts.values())
    print(f"Total titles processed: {total_titles}")
    print(f"Unique first words found: {len(final_counts)}\n")

    # ── 5. Extract Top 10 ─────────────────────────
    top_10_tuples = final_counts.most_common(10)
    top_10 = [word for word, _ in top_10_tuples]

    print("=== Top 10 Most Frequent First Words ===")
    for rank, (word, count) in enumerate(top_10_tuples, 1):
        print(f"  {rank:>2}. {word:<20} {count} occurrences")

    # ── 6. Verify with the server ─────────────────
    print()
    if top_10:
        verify_top_10(STUDENT_ID, top_10)
    else:
        print("No words were collected – check connectivity and student ID.")
