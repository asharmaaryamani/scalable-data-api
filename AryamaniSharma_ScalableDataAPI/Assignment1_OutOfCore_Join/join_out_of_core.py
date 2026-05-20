"""
Assignment 1: Out-of-Core Data Join
====================================
Strategy: Hash-based External Join

Approach:
---------
Since we cannot load both 500MB files into 256MB RAM simultaneously,
we use a two-phase hash-partition strategy:

Phase 1 - Partition:
  Read each file in small chunks and write each row into one of N
  partition "buckets" based on hash(user_id) % N.
  After Phase 1, rows with the same user_id are guaranteed to be in
  the same bucket on both sides.

Phase 2 - Join per bucket:
  Each bucket is small enough to fit entirely in memory.
  We load both sides of a bucket, do an in-memory join, and append
  the result rows to result.csv.

This keeps peak RAM usage to roughly (total_size / N) per side,
which we can tune with NUM_PARTITIONS.

Memory budget analysis (256 MB):
  users.csv      ~500 MB  →  ~500/40 = 12.5 MB per bucket (users side)
  transactions.csv ~500 MB → ~500/40 = 12.5 MB per bucket (tx side)
  Peak in Phase 2 = ~25 MB per bucket  ← well within 256 MB budget
  OS + Python overhead ~80 MB, leaving ~176 MB for data → safe.
"""

import os
import csv
import hashlib
import logging
import tempfile
import shutil
from pathlib import Path

# ── Configuration ────────────────────────────────────────────────────────────
NUM_PARTITIONS   = 40        # Number of hash buckets
CHUNK_SIZE       = 50_000    # Rows read at once during partitioning phase
USERS_FILE       = "users.csv"
TRANSACTIONS_FILE = "transactions.csv"
OUTPUT_FILE      = "result.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def bucket_for(user_id: str, n: int) -> int:
    """Map a user_id string to a partition bucket index [0, n)."""
    return int(hashlib.md5(user_id.encode()).hexdigest(), 16) % n


def partition_file(src_path: str, key_col: str, tmp_dir: str,
                   prefix: str, n: int) -> list[list]:
    """
    Stream-read src_path in chunks, writing each row to its bucket file.
    Returns a list of per-bucket csv.writer handles (files are kept open
    during partitioning for efficiency, then closed).

    Peak memory ≈ CHUNK_SIZE rows × row_width  (one chunk at a time).
    """
    bucket_files  = [
        open(os.path.join(tmp_dir, f"{prefix}_bucket_{i}.csv"), "w",
             newline="", buffering=1 << 20)   # 1 MB write buffer
        for i in range(n)
    ]
    bucket_writers = [csv.writer(f) for f in bucket_files]
    header_written = [False] * n

    row_count = 0
    with open(src_path, newline="", buffering=1 << 20) as fh:
        reader = csv.DictReader(fh)
        chunk  = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= CHUNK_SIZE:
                _flush_chunk(chunk, key_col, n, bucket_writers,
                             header_written, list(reader.fieldnames))
                row_count += len(chunk)
                chunk = []
        if chunk:
            _flush_chunk(chunk, key_col, n, bucket_writers,
                         header_written, list(reader.fieldnames))
            row_count += len(chunk)

    for f in bucket_files:
        f.close()

    log.info("  Partitioned %s  (%d rows  →  %d buckets)", src_path, row_count, n)
    return list(reader.fieldnames) if hasattr(reader, "fieldnames") else []


def _flush_chunk(chunk, key_col, n, writers, header_written, fieldnames):
    for row in chunk:
        b = bucket_for(row[key_col], n)
        if not header_written[b]:
            writers[b].writerow(fieldnames)
            header_written[b] = True
        writers[b].writerow([row[f] for f in fieldnames])


def join_buckets(tmp_dir: str, n: int, output_path: str):
    """
    Phase 2: For each bucket, load both sides into memory, inner-join on
    user_id, write matched rows to output_path.

    Peak memory ≈ size_of_one_bucket_users + size_of_one_bucket_transactions.
    """
    total_rows = 0
    first_bucket = True

    for i in range(n):
        u_path = os.path.join(tmp_dir, f"users_bucket_{i}.csv")
        t_path = os.path.join(tmp_dir, f"transactions_bucket_{i}.csv")

        # Build in-memory lookup for the *smaller* side (users)
        users_map: dict[str, list] = {}
        u_header = []
        if os.path.exists(u_path):
            with open(u_path, newline="") as fh:
                reader = csv.DictReader(fh)
                u_header = list(reader.fieldnames)
                for row in reader:
                    users_map[row["user_id"]] = row

        if not users_map or not os.path.exists(t_path):
            continue

        # Stream transactions bucket; emit matched rows
        with open(t_path, newline="") as fh:
            t_reader = csv.DictReader(fh)

            if t_reader.fieldnames is None:
               continue

            t_header = list(t_reader.fieldnames)

            # Build merged header once
            merged_header = u_header + [c for c in t_header if c != "user_id"]

            mode = "w" if first_bucket else "a"
            with open(output_path, mode, newline="") as out_fh:
                writer = csv.writer(out_fh)
                if first_bucket:
                    writer.writerow(merged_header)
                    first_bucket = False

                for t_row in t_reader:
                    uid = t_row["user_id"]
                    if uid in users_map:
                        u_row = users_map[uid]
                        merged = [u_row[c] for c in u_header] + \
                                 [t_row[c] for c in t_header if c != "user_id"]
                        writer.writerow(merged)
                        total_rows += 1

        if (i + 1) % 10 == 0:
            log.info("  Joined %d / %d buckets  (%d output rows so far)",
                     i + 1, n, total_rows)

    return total_rows


# ── Public entry-point ────────────────────────────────────────────────────────

def run_join(users_path: str = USERS_FILE,
             transactions_path: str = TRANSACTIONS_FILE,
             output_path: str = OUTPUT_FILE) -> int:
    """
    Perform an out-of-core INNER JOIN and write result to output_path.
    Returns the total number of joined rows written.
    """
    log.info("=== Assignment 1: Out-of-Core Join started ===")
    log.info("Users file       : %s", users_path)
    log.info("Transactions file: %s", transactions_path)
    log.info("Output file      : %s", output_path)
    log.info("Partitions (N)   : %d", NUM_PARTITIONS)
    log.info("Chunk size       : %d rows", CHUNK_SIZE)

    tmp_dir = tempfile.mkdtemp(prefix="oc_join_")
    try:
        # ── Phase 1: Partition both files ──────────────────────────────────
        log.info("--- Phase 1: Partitioning ---")
        partition_file(users_path,        "user_id", tmp_dir, "users",        NUM_PARTITIONS)
        partition_file(transactions_path, "user_id", tmp_dir, "transactions", NUM_PARTITIONS)

        # ── Phase 2: Join each bucket pair ─────────────────────────────────
        log.info("--- Phase 2: Joining buckets ---")
        total = join_buckets(tmp_dir, NUM_PARTITIONS, output_path)

        log.info("=== Join complete — %d rows written to %s ===", total, output_path)
        return total

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_join()
