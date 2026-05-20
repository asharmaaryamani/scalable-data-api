"""
main.py — Unified FastAPI app exposing BOTH approaches
=======================================================
Run:
    uvicorn main:app --host 0.0.0.0 --port 8000

Endpoints:
  POST /approach1/trigger-join   — BackgroundTasks approach
  GET  /approach1/jobs/{job_id}  — Status for approach 1 job

  POST /approach2/trigger-join   — ProcessPoolExecutor approach
  GET  /approach2/jobs/{job_id}  — Status for approach 2 job

  GET  /health                   — Quick health check
  GET  /docs                     — Swagger UI
"""

import asyncio
import uuid
import logging
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel

from join_out_of_core import run_join, USERS_FILE, TRANSACTIONS_FILE, OUTPUT_FILE

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main")

# ── Shared state ──────────────────────────────────────────────────────────────
registry_a1: dict[str, dict[str, Any]] = {}   # Approach 1 jobs
registry_a2: dict[str, dict[str, Any]] = {}   # Approach 2 jobs

MAX_WORKERS = 2
_pool: ProcessPoolExecutor | None = None


# ── Lifespan (startup / shutdown) ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    _pool = ProcessPoolExecutor(max_workers=MAX_WORKERS)
    log.info("ProcessPoolExecutor created (max_workers=%d)", MAX_WORKERS)
    yield
    _pool.shutdown(wait=False)
    log.info("ProcessPoolExecutor shut down")


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Scalable Data Processing API",
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


# ═══════════════════════════════════════════════════════════════════════════════
#  APPROACH 1 — FastAPI BackgroundTasks
# ═══════════════════════════════════════════════════════════════════════════════

def _a1_worker(job_id: str, users: str, transactions: str, output: str):
    """Runs in a Starlette background thread."""
    log.info("[A1][job:%s] Join STARTED", job_id)
    registry_a1[job_id]["status"]     = "running"
    registry_a1[job_id]["started_at"] = datetime.utcnow().isoformat()
    try:
        rows = run_join(users, transactions, output)
        registry_a1[job_id].update(
            status="completed",
            finished_at=datetime.utcnow().isoformat(),
            rows_written=rows,
        )
        log.info("[A1][job:%s] Join FINISHED — %d rows written to %s",
                 job_id, rows, output)
    except Exception as exc:
        registry_a1[job_id].update(status="failed", error=str(exc))
        log.exception("[A1][job:%s] Join FAILED: %s", job_id, exc)


@app.post("/approach1/trigger-join", response_model=TriggerResponse,
          status_code=202, tags=["Approach 1 — BackgroundTasks"])
async def a1_trigger(background_tasks: BackgroundTasks):
    """
    **Approach 1 — FastAPI BackgroundTasks**

    Queues the join in a Starlette background thread.
    Returns HTTP 202 with a `job_id` immediately.
    """
    jid = str(uuid.uuid4())
    registry_a1[jid] = dict(status="queued", started_at=None,
                             finished_at=None, rows_written=None, error=None)
    background_tasks.add_task(
        _a1_worker, jid, USERS_FILE, TRANSACTIONS_FILE, OUTPUT_FILE
    )
    log.info("[A1][job:%s] Queued", jid)
    return TriggerResponse(job_id=jid, status="queued",
                           message="Queued. Poll /approach1/jobs/{job_id} for status.")


@app.get("/approach1/jobs/{job_id}", response_model=JobStatusResponse,
         tags=["Approach 1 — BackgroundTasks"])
async def a1_status(job_id: str):
    """Poll the status of an Approach-1 join job."""
    if job_id not in registry_a1:
        raise HTTPException(404, "Job not found")
    return JobStatusResponse(job_id=job_id, **registry_a1[job_id])


# ═══════════════════════════════════════════════════════════════════════════════
#  APPROACH 2 — ProcessPoolExecutor
# ═══════════════════════════════════════════════════════════════════════════════

def _a2_process_worker(users: str, transactions: str, output: str) -> int:
    """Runs in a separate OS process. Returns row count."""
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [WORKER] %(message)s")
    return run_join(users, transactions, output)


async def _a2_watch(job_id: str, future: asyncio.Future):
    try:
        rows = await future
        registry_a2[job_id].update(
            status="completed",
            finished_at=datetime.utcnow().isoformat(),
            rows_written=rows,
        )
        log.info("[A2][job:%s] Join FINISHED — %d rows written", job_id, rows)
    except Exception as exc:
        registry_a2[job_id].update(status="failed", error=str(exc))
        log.exception("[A2][job:%s] Join FAILED: %s", job_id, exc)


@app.post("/approach2/trigger-join", response_model=TriggerResponse,
          status_code=202, tags=["Approach 2 — ProcessPoolExecutor"])
async def a2_trigger():
    """
    **Approach 2 — ProcessPoolExecutor**

    Submits the join to a process pool (true CPU parallelism, GIL-free).
    Returns HTTP 202 with a `job_id` immediately.
    """
    jid = str(uuid.uuid4())
    registry_a2[jid] = dict(status="running",
                             started_at=datetime.utcnow().isoformat(),
                             finished_at=None, rows_written=None, error=None)
    loop   = asyncio.get_event_loop()
    future = loop.run_in_executor(
        _pool, _a2_process_worker, USERS_FILE, TRANSACTIONS_FILE, OUTPUT_FILE
    )
    asyncio.create_task(_a2_watch(jid, future))
    log.info("[A2][job:%s] Submitted to ProcessPool", jid)
    return TriggerResponse(
        job_id=jid, status="running",
        message="Running in process pool. Poll /approach2/jobs/{job_id} for status.",
    )


@app.get("/approach2/jobs/{job_id}", response_model=JobStatusResponse,
         tags=["Approach 2 — ProcessPoolExecutor"])
async def a2_status(job_id: str):
    """Poll the status of an Approach-2 join job."""
    if job_id not in registry_a2:
        raise HTTPException(404, "Job not found")
    return JobStatusResponse(job_id=job_id, **registry_a2[job_id])


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok", "pool_workers": MAX_WORKERS}


# ── Dev runner ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
