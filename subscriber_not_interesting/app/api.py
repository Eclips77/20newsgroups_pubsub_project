import threading
from typing import List, Dict, Any
from fastapi import FastAPI

from .consumer import NotInterestingConsumerService, get_collection

app = FastAPI(title="Not-Interesting Subscriber Service", version="1.0.0")

_service: NotInterestingConsumerService | None = None

_running = False
_worker: threading.Thread | None = None

def _loop():
    assert _service is not None
    while _running:
        _service.consume_once()

@app.on_event("startup")
def _on_startup():
    """
    Start the background consumer loop.
    """
    global _running, _worker, _service
    _service = NotInterestingConsumerService()
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)