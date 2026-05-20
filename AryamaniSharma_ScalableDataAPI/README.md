# Scalable Data Processing API
**Engineering Assignment Submission**

---

## Overview

This submission solves both parts of the assignment:

| Part | Problem | Solution |
|---|---|---|
| Assignment 1 | Inner join two 500 MB CSVs inside 256 MB RAM | Hash-partition external join (never loads more than ~110 MB at once) |
| Assignment 2 | Non-blocking API to trigger that join | FastAPI with 2 background concurrency approaches |

---

## Project Structure

```
.
├── Assignment1_OutOfCore_Join/
│   ├── join_out_of_core.py        # Core join engine (Assignment 1)
│   └── generate_data.py           # Generates users.csv + transactions.csv
│
├── Assignment2_NonBlocking_API/
│   ├── main.py                    # Unified FastAPI server (run this)
│   ├── approach1_background_tasks.py   # Approach 1 implementation
│   ├── approach2_process_pool.py       # Approach 2 implementation
│   └── test_assignment.py         # Automated correctness tests
│
└── requirements.txt
```

---

## Assignment 1: Out-of-Core Data Join

### The Problem
Perform an `INNER JOIN` on `users.csv` (~500 MB, 5M rows) and `transactions.csv`
(~500 MB, 10M rows) on `user_id` — inside a container limited to **256 MB of RAM**.

Loading both files simultaneously would require ~1 GB, so a direct `pandas.merge()` is impossible.

### Solution: Hash-Partition External Join

The algorithm runs in two phases:

#### Phase 1 — Partition
Both files are streamed in chunks of 50,000 rows at a time.
Each row is routed to one of **40 hash buckets** based on:

```python
bucket = int(md5(user_id).hexdigest(), 16) % 40
```

After Phase 1, every row with the same `user_id` is **guaranteed to be in the same bucket** on both sides.

#### Phase 2 — Join per bucket
For each of the 40 bucket pairs:
1. Load the users bucket into a `dict` (build side) — fits in memory.
2. Stream the transactions bucket row-by-row (probe side).
3. Emit matched rows directly to `result.csv`.

#### Memory Budget Breakdown

| Component | RAM Used |
|---|---|
| Python + OS overhead | ~80 MB |
| One users bucket (500 MB ÷ 40) | ~12.5 MB |
| One transactions bucket (500 MB ÷ 40) | ~12.5 MB |
| I/O buffers | ~5 MB |
| **Peak total** | **~110 MB ✅ (well within 256 MB)** |

#### How to Run

```bash
python generate_data.py          # Step 1: create users.csv + transactions.csv
python join_out_of_core.py       # Step 2: produces result.csv
```

#### Sample Output Logs

```
10:12:01 [INFO] === Assignment 1: Out-of-Core Join started ===
10:12:01 [INFO] Partitions (N): 40 | Chunk size: 50,000 rows
10:12:01 [INFO] --- Phase 1: Partitioning ---
10:12:01 [INFO]   Partitioned users.csv        (5000000 rows → 40 buckets)
10:13:10 [INFO]   Partitioned transactions.csv (10000000 rows → 40 buckets)
10:13:10 [INFO] --- Phase 2: Joining buckets ---
10:13:20 [INFO]   Joined 10 / 40 buckets  (2461823 output rows so far)
10:14:30 [INFO]   Joined 20 / 40 buckets  (4923002 output rows so far)
10:15:42 [INFO] === Join complete — 9843117 rows written to result.csv ===
```

---

## Assignment 2: Non-Blocking API

### The Problem
The join takes several minutes and uses significant CPU. Running it synchronously
would block the web server and cause client timeouts under concurrent load.

### Solution: Async job submission with two approaches

A `POST /trigger-join` endpoint returns a `job_id` immediately (HTTP 202 Accepted)
while the join runs in the background. Clients poll `GET /jobs/{job_id}` for status.

```
Client                    Server
  │                          │
  │  POST /trigger-join      │
  │─────────────────────────>│  Returns job_id instantly
  │  202 { job_id: "abc" }   │  Join starts in background
  │<─────────────────────────│
  │                          │  ... join running ...
  │  GET /jobs/abc           │
  │─────────────────────────>│
  │  { status: "running" }   │
  │<─────────────────────────│
  │                          │  ... join finishes ...
  │  GET /jobs/abc           │
  │─────────────────────────>│
  │  { status: "completed",  │
  │    rows_written: 9843117}│
  │<─────────────────────────│
```

---

### Approach 1: FastAPI `BackgroundTasks`

**File:** `approach1_background_tasks.py`

FastAPI's built-in `BackgroundTasks` schedules the join in a **background thread**
in the same process as the web server. The endpoint returns immediately; the join
executes concurrently via Starlette's thread pool.

#### Logging
```
08:41:12 [INFO] [job:abc-123] Queued via BackgroundTasks
08:41:12 [INFO] [job:abc-123] Background join STARTED
08:41:45 [INFO] [job:abc-123] Background join FINISHED — 9843117 rows written to result.csv
```

#### Pros
- Zero extra infrastructure — no Redis, Celery, or Docker needed
- Simplest deployment: a single `uvicorn` command
- Built into FastAPI/Starlette, no extra dependencies
- Easy in-process job state sharing

#### Cons
- Shares the process with the web server — CPU-heavy join can slow HTTP response times
- Job state is lost if the server restarts
- Does not scale across multiple workers or machines
- No retry, priority queue, or dead-letter support
- Python GIL limits true CPU parallelism in threads

**Best for:** Low-traffic scenarios, prototypes, or I/O-bound background tasks.

---

### Approach 2: `ProcessPoolExecutor`

**File:** `approach2_process_pool.py`

A `ProcessPoolExecutor(max_workers=2)` is created at startup. When the endpoint
is hit, the join is submitted via `loop.run_in_executor()` — which is asyncio-native
and non-blocking. Each worker is a **separate OS process**, bypassing Python's GIL
entirely for true CPU parallelism.

#### Logging
```
08:41:12 [INFO] [job:xyz-456] Submitted to ProcessPoolExecutor
08:41:12 [INFO] [WORKER] === Assignment 1: Out-of-Core Join started ===
08:41:12 [INFO] [WORKER] --- Phase 1: Partitioning ---
08:41:45 [INFO] [job:xyz-456] Join FINISHED — 9843117 rows written
```

#### Pros
- True CPU parallelism — GIL bypassed entirely (separate OS processes)
- Zero extra infrastructure — still no Redis or Celery needed
- Worker crash is isolated and does not kill the web server
- Multiple concurrent joins can run in parallel
- asyncio-native via `run_in_executor` — event loop stays responsive

#### Cons
- Higher memory footprint — each worker is a full Python process
- Job state still lost on server restart
- Function arguments serialised via pickle (IPC overhead)
- Pool size must be tuned carefully to respect the 256 MB RAM budget
- Cannot distribute work across multiple machines without an external broker

**Best for:** CPU-heavy workloads on a single server where true parallelism is needed
without the operational overhead of a full task queue (Celery + Redis).

---

### Approach Comparison

| Criterion | Approach 1 (BackgroundTasks) | Approach 2 (ProcessPoolExecutor) |
|---|---|---|
| CPU parallelism | Limited by GIL | True parallel (separate processes) |
| Infrastructure needed | None | None |
| Memory per worker | Low (shared process) | Higher (one process each) |
| Worker crash isolation | No | Yes |
| Horizontal scalability | No | No |
| Job persistence on restart | No | No |
| Implementation complexity | Simplest | Moderate |

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/approach1/trigger-join` | Trigger join via BackgroundTasks |
| `GET` | `/approach1/jobs/{job_id}` | Poll status of Approach 1 job |
| `POST` | `/approach2/trigger-join` | Trigger join via ProcessPool |
| `GET` | `/approach2/jobs/{job_id}` | Poll status of Approach 2 job |
| `GET` | `/health` | Health check |
| `GET` | `/docs` | Swagger UI (interactive) |

### Job Status Response

```json
{
  "job_id": "abc-123",
  "status": "completed",
  "started_at": "2024-01-15T10:41:12",
  "finished_at": "2024-01-15T10:42:45",
  "rows_written": 9843117,
  "error": null
}
```

Status values: `queued` → `running` → `completed` / `failed`

---

## How to Run

```bash
# Install dependencies
pip install -r requirements.txt

# Assignment 1 — run the join directly
cd Assignment1_OutOfCore_Join
python generate_data.py       # generates CSVs (~3-5 mins)
python join_out_of_core.py    # produces result.csv

# Assignment 2 — start the API server
cd Assignment2_NonBlocking_API
uvicorn main:app --host 0.0.0.0 --port 8000

# Open http://localhost:8000/docs for Swagger UI

# Run tests
python test_assignment.py     # all tests pass
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.10+ | Core language |
| FastAPI | Web framework |
| Uvicorn | ASGI server |
| `concurrent.futures` | ProcessPoolExecutor (Approach 2) |
| `csv` / `hashlib` | Out-of-core join engine (stdlib only) |
| Pandas + NumPy | Data generation script only |

> Note: The join engine (`join_out_of_core.py`) uses only Python standard library
> (`csv`, `hashlib`, `tempfile`) — no pandas — to stay within the 256 MB RAM constraint.
