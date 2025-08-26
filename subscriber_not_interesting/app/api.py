from typing import List, Dict, Any
from fastapi import FastAPI
from .consumer import NotInterestingConsumerService

app = FastAPI(title="Not-Interesting Subscriber Service", version="1.0.0")
_service = NotInterestingConsumerService()


@app.get("/consume")
def consume():
    """
    Returns that the data consume successfully
    """
    _service.consume_once()
    return {"status": "consumed"}

@app.get("/messages")
def messages() -> List[Dict[str, Any]]:
    """
    Return all stored messages for this subscriber (most recent first).
    """
    coll = _service.collection
    docs = list(coll.find().sort("timestamp", -1).limit(10))
    for d in docs:
        d["_id"] = str(d["_id"])
    return docs

@app.get("/health")
def health() -> Dict[str, str]:
    """
    Health check endpoint.
    """
    return {"status": "ok"}


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8002)