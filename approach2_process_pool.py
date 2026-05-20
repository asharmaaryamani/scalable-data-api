"""
Assignment 2 — Approach 2: concurrent.futures ProcessPoolExecutor
==================================================================

How it works:
  A ProcessPoolExecutor is created at startup with a configurable number
  of worker *processes* (not threads).  When POST /trigger-join is called,
  the join function is submitted as a Future to the pool.  The endpoint
  returns a job_id immediately.  A lightweight asyncio task polls the
  Future periodically and updates the shared job registry.

  Because each worker is a separate OS process, Python's GIL is bypassed
  completely — CPU-intensive work runs in true parallel without starving
  the asyncio event loop.

Pros:
  ✔ True CPU parallelism — bypasses Python's GIL entirely.
  ✔ Still zero extra infrastructure (no Redis, no Celery, no Docker).
  ✔ Worker processes are isolated; a crash in the join does not kill
    the web server process.
  ✔ Multiple concurrent joins can run in parallel (pool size controls it).
  ✔ asyncio-native via loop.run_in_executor — non-blocking HTTP handling.

Cons:
  ✗ Heavier than BackgroundTasks — each worker is a full Python process
    (startup cost, higher memory footprint per worker).
  ✗ State still lives in-process — lost if the web server restarts.
  ✗ IPC overhead: arguments and return values are pickle-serialised.
  ✗ Pool size must be chosen carefully; too many workers exhaust RAM.
  ✗ Does not scale across multiple machines without an external broker.

Best for: CPU-heavy workloads on a single server where you want true
          parallelism but don't want to operate a Celery/Redis stack.
"""

import asyncio
import uuid
import logging
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

from join_out_of_core import run_join, USERS_FILE, TRANSACTIONS_FILE, OUTPUT_FILE

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("approach2")

# ── Config ────────────────────────────────────────────────────────────────────
MAX_WORKERS = 2   # Limit parallelism to protect the 256 MB RAM budget.

# ── In-process job registry ───────────────────────────────────────────────────
job_registry: dict[str, dict[str, Any]] = {}

# ── Process pool (created at app startup) ─────────────────────────────────────
_pool: ProcessPoolExecutor | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create the process pool on startup; shut it down cleanly on exit."""
    global _pool
    _pool = ProcessPoolExecutor(max_workers=MAX_WORKERS)
    log.info("ProcessPoolExecutor started (max_workers=%d)", MAX_WORKERS)
    yield
    _pool.shutdown(wait=False)
    log.info("ProcessPoolExecutor shut down")


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="Scalable Data Join API — Approach 2 (ProcessPoolExecutor)",
    description=__doc__,
    version="1.0.0",
    lifespan=lifespan,
)


# ── Schemas ───────────────────────────────────────────────────────────────────
class TriggerResponse(BaseModel):
    job_id: str
    status: str
    message: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    started_at: str | None = None
    finished_at: str | None = None
    rows_written: int | None = None
    error: str | None = None


# ── Worker function (runs in a child process) ─────────────────────────────────
def _worker(users: str, transactions: str, output: str) -> int:
    """
    Executed in a separate OS process by the pool.
    Returns the number of rows written (passed back via IPC / pickle).
    """
    # Re-configure logging in the child process
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] WORKER — %(message)s",
        datefmt="%H:%M:%S",
    )
    return run_join(
        users_path=users,
        transactions_path=transactions,
        output_path=output,
    )


# ── Async watcher task ────────────────────────────────────────────────────────
async def _watch_future(job_id: str, future: asyncio.Future):
    """
    Awaits the Future in a non-blocking way and updates the job registry
    when the result is available.
    """
    try:
        rows = await future
        job_registry[job_id]["status"]       = "completed"
        job_registry[job_id]["finished_at"]  = datetime.utcnow().isoformat()
        job_registry[job_id]["rows_written"] = rows
        log.info("[job:%s] ProcessPool job FINISHED — %d rows written", job_id, rows)

    except Exception as exc:
        job_registry[job_id]["status"] = "failed"
        job_registry[job_id]["error"]  = str(exc)
        log.exception("[job:%s] ProcessPool job FAILED: %s", job_id, exc)


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.post("/trigger-join", response_model=TriggerResponse, status_code=202)
async def trigger_join():
    """
    Submits the join to the process pool and returns HTTP 202 immediately.
    """
    job_id = str(uuid.uuid4())
    job_registry[job_id] = {
        "status":       "running",
        "started_at":   datetime.utcnow().isoformat(),
        "finished_at":  None,
        "rows_written": None,
        "error":        None,
    }

    loop   = asyncio.get_event_loop()
    future = loop.run_in_executor(
        _pool, _worker, USERS_FILE, TRANSACTIONS_FILE, OUTPUT_FILE
    )

    # Fire-and-forget watcher so the registry is updated when done
    asyncio.create_task(_watch_future(job_id, future))

    log.info("[job:%s] Submitted to ProcessPoolExecutor", job_id)
    return TriggerResponse(
        job_id=job_id,
        status="running",
        message="Join job started in a separate process. Poll /jobs/{job_id} for status.",
    )


@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Poll the status of a previously submitted join job."""
    if job_id not in job_registry:
        raise HTTPException(status_code=404, detail="Job not found")
    info = job_registry[job_id]
    return JobStatusResponse(job_id=job_id, **info)


@app.get("/health")
async def health():
    return {"status": "ok", "approach": "ProcessPoolExecutor"}


# ── Dev runner ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("approach2_process_pool:app", host="0.0.0.0", port=8002, reload=False)
