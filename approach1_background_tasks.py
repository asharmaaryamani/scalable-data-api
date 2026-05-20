"""
Assignment 2 — Approach 1: FastAPI Built-in BackgroundTasks
============================================================

How it works:
  FastAPI's BackgroundTasks runs the job in the *same process* as the web
  server, using a thread pool that Starlette manages under the hood.
  The endpoint returns a job_id immediately; the heavy join function
  executes in a background thread.

Pros:
  ✔ Zero extra infrastructure — no Redis, no Celery worker, no Docker.
  ✔ Simple to deploy: a single `uvicorn` command is all you need.
  ✔ Built into FastAPI/Starlette, so no extra pip dependencies.
  ✔ In-process state sharing (job_status dict) is trivial.

Cons:
  ✗ Shares the process with the web server — a CPU-intensive join can
    starve asyncio and slow down other HTTP requests.
  ✗ If the server restarts, all job state and in-flight jobs are lost.
  ✗ Does not scale across multiple worker processes or machines.
  ✗ No retry / dead-letter / priority queue support.
  ✗ Thread concurrency is limited by Python's GIL for CPU-bound work.

Best for: Low traffic, quick prototypes, or I/O-bound background tasks.
"""

import uuid
import logging
from datetime import datetime
from typing import Any

from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel

from join_out_of_core import run_join, USERS_FILE, TRANSACTIONS_FILE, OUTPUT_FILE

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("approach1")

# ── In-process job registry ───────────────────────────────────────────────────
job_registry: dict[str, dict[str, Any]] = {}

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="Scalable Data Join API — Approach 1 (BackgroundTasks)",
    description=__doc__,
    version="1.0.0",
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


# ── Background worker function ────────────────────────────────────────────────
def perform_join(job_id: str, users: str, transactions: str, output: str):
    """
    Runs in a background thread.  Updates job_registry so the status
    endpoint can report progress.
    """
    log.info("[job:%s] Background join STARTED", job_id)
    job_registry[job_id]["status"]     = "running"
    job_registry[job_id]["started_at"] = datetime.utcnow().isoformat()

    try:
        rows = run_join(
            users_path=users,
            transactions_path=transactions,
            output_path=output,
        )
        job_registry[job_id]["status"]      = "completed"
        job_registry[job_id]["finished_at"] = datetime.utcnow().isoformat()
        job_registry[job_id]["rows_written"] = rows
        log.info("[job:%s] Background join FINISHED — %d rows written to %s",
                 job_id, rows, output)

    except Exception as exc:
        job_registry[job_id]["status"] = "failed"
        job_registry[job_id]["error"]  = str(exc)
        log.exception("[job:%s] Background join FAILED: %s", job_id, exc)


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.post("/trigger-join", response_model=TriggerResponse, status_code=202)
async def trigger_join(background_tasks: BackgroundTasks):
    """
    Immediately returns a job_id (HTTP 202 Accepted) and schedules the
    out-of-core join as a background task.
    """
    job_id = str(uuid.uuid4())
    job_registry[job_id] = {
        "status":      "queued",
        "started_at":  None,
        "finished_at": None,
        "rows_written": None,
        "error":       None,
    }
    background_tasks.add_task(
        perform_join, job_id, USERS_FILE, TRANSACTIONS_FILE, OUTPUT_FILE
    )
    log.info("[job:%s] Queued via BackgroundTasks", job_id)
    return TriggerResponse(
        job_id=job_id,
        status="queued",
        message="Join job queued. Poll /jobs/{job_id} for status.",
    )


@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Poll the status of a previously submitted join job."""
    if job_id not in job_registry:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Job not found")
    info = job_registry[job_id]
    return JobStatusResponse(job_id=job_id, **info)


@app.get("/health")
async def health():
    return {"status": "ok", "approach": "BackgroundTasks"}


# ── Dev runner ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("approach1_background_tasks:app", host="0.0.0.0", port=8001, reload=False)
