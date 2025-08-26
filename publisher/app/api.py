from typing import Dict, Any
from fastapi import FastAPI, HTTPException
from .flow_manager import FlowManager
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Publisher Service", version="1.0.0")

# Initialize FlowManager with proper error handling
try:
    _flow_manager = FlowManager()
    logger.info("Publisher service initialized successfully")
except Exception as e:
    logger.error("Failed to initialize Publisher service: %s", str(e), exc_info=True)
    _flow_manager = None


@app.get("/publish")
def publish_messages() -> Dict[str, Any]:
    """
    Fetch and publish 20 messages (10 per group) to Kafka.

    Returns:
        JSON with number of messages published per topic.
        
    Raises:
        HTTPException: If publishing fails
    """
    logger.info("Received publish request")
    
    if _flow_manager is None:
        logger.error("FlowManager is not initialized")
        raise HTTPException(status_code=500, detail="Service not properly initialized")
    
    try:
        result = _flow_manager.fetch_and_publish()
        logger.info("Successfully published messages: %s", result)
        return result
        
    except ValueError as e:
        logger.error("Validation error during publishing: %s", str(e))
        raise HTTPException(status_code=400, detail=str(e))
        
    except Exception as e:
        logger.error("Unexpected error during publishing: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error during publishing")


@app.get("/health")
def health_check() -> Dict[str, str]:
    """
    Health check endpoint.
    
    Returns:
        Health status of the service
    """
    if _flow_manager is None:
        return {"status": "unhealthy", "message": "FlowManager not initialized"}
    
    return {"status": "healthy", "message": "Publisher service is running"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)