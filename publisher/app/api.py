from typing import Dict, Any
from fastapi import FastAPI
from .flow_manager import FlowManager
import logging
logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.INFO)


app = FastAPI(title="Publisher Service", version="1.0.0")
_flow_manager = FlowManager()

@app.get("/publish")
def publish_messages() -> Dict[str, Any]:
    """
    Fetch and publish 20 messages (10 per group) to Kafka.

    Returns:
        JSON with number of messages published per topic.
    """
    result = _flow_manager.fetch_and_publish()
    logging.info("Published messages: %s", result)
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)