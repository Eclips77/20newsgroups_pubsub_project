
"""
api.py (interesting)
--------------------
FastAPI app for the 'interesting' subscriber.
Provides endpoints to fetch stored messages and a health check.

Endpoints:
    - GET /messages : Return all stored messages (most recent first)
    - GET /health   : Health check
"""

import threading
from typing import List, Dict, Any
from fastapi import FastAPI

from .consumer import build_consumer, get_collection, consume_once

app = FastAPI(title="Interesting Subscriber Service", version="1.0.0")

_running = False
_worker: threading.Thread | None = None

def _loop():
    consumer = build_consumer()
    coll = get_collection()
    while _running:
        consume_once(consumer, coll)

@app.on_event("startup")
def _on_startup():
    """
    Start the background consumer loop.
    """
    global _running, _worker
    _running = True
    _worker = threading.Thread(target=_loop, daemon=True)
    _worker.start()

@app.on_event("shutdown")
def _on_shutdown():
    """
    Stop the background consumer loop.
    """
    global _running, _worker
    _running = False
    if _worker and _worker.is_alive():
        _worker.join(timeout=5)

@app.get("/messages")
def messages() -> List[Dict[str, Any]]:
    """
    Return all stored messages for this subscriber (most recent first).
    """
    coll = get_collection()
    docs = list(coll.find().sort("timestamp", -1))
    for d in docs:
        d["_id"] = str(d["_id"])
    return docs

@app.get("/health")
def health() -> Dict[str, str]:
    """
    Health check endpoint.
    """
    return {"status": "ok"}
