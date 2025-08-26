from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException
from .consumer import InterestingConsumerService
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Interesting Subscriber Service", version="1.0.0")

try:
    _service = InterestingConsumerService()
    logger.info("Interesting subscriber service initialized successfully")
except Exception as e:
    logger.error("Failed to initialize Interesting subscriber service: %s", str(e), exc_info=True)
    _service = None


@app.get("/consume")
def consume() -> Dict[str, Any]:
    """
    Consume messages from Kafka and store in MongoDB.
    
    Returns:
        Dict containing consumption summary
        
    Raises:
        HTTPException: If consumption fails
    """
    logger.info("Received consume request")
    
    if _service is None:
        logger.error("Consumer service is not initialized")
        raise HTTPException(status_code=500, detail="Service not properly initialized")
    
    try:
        result = _service.consume_once()
        logger.info("Consumption completed: %s", result)
        return {"status": "consumed", "details": result}
        
    except Exception as e:
        logger.error("Error during consumption: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to consume messages")


@app.get("/messages")
def messages() -> List[Dict[str, Any]]:
    """
    Return all stored messages for this subscriber (most recent first).
    
    Returns:
        List of recent messages
        
    Raises:
        HTTPException: If database query fails
    """
    logger.info("Received messages request")
    
    if _service is None:
        logger.error("Consumer service is not initialized")
        raise HTTPException(status_code=500, detail="Service not properly initialized")
    
    try:
        coll = _service.collection
        docs = list(coll.find().sort("timestamp", -1).limit(10))
        
        # Convert ObjectId to string for JSON serialization
        for d in docs:
            d["_id"] = str(d["_id"])
        
        logger.info("Retrieved %d messages", len(docs))
        return docs
        
    except Exception as e:
        logger.error("Error retrieving messages: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve messages")


@app.get("/health")
def health() -> Dict[str, str]:
    """
    Health check endpoint.
    
    Returns:
        Health status of the service
    """
    if _service is None:
        return {"status": "unhealthy", "message": "Consumer service not initialized"}
    
    try:
        # Test MongoDB connection
        _service.collection.database.command('ping')
        return {"status": "healthy", "message": "Interesting subscriber service is running"}
        
    except Exception as e:
        logger.warning("Health check failed: %s", str(e))
        return {"status": "degraded", "message": f"Service running but database issue: {str(e)}"}


@app.on_event("shutdown")
def shutdown_event():
    """
    Cleanup resources on application shutdown.
    """
    logger.info("Shutting down interesting subscriber service")
    if _service:
        _service.close()


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8001)