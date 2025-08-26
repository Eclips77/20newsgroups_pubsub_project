from pymongo import MongoClient, ASCENDING, errors
import logging
from . import config
logger = logging.getLogger(__name__)


class MongoDAL:

    def __init__(self):
        """
        Initialize database connection
        """
        self.client = None
        self._coll = None

    def connect(self):
        """
        Connect to the MongoDB database
        """
        try:
            self.client = MongoClient(config.MONGO_URI, tz_aware=True, uuidRepresentation="standard")
            self._coll = self.client[config.MONGO_DB][config.COLLECTION_NAME]
            logger.info("Connected to MongoDB at %s", config.MONGO_URI)
            return self._coll
        
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            logger.error(f"Database name: {config.MONGO_DB}")
            raise ConnectionError(f"Failed to connect to MongoDB: {e}")

    def send_message(self, docs):
        """
        Send docs to the MongoDB collection
        """
        if self._coll is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        
        try:
            if isinstance(docs, list):
                result = self._coll.insert_many(docs)
                logger.info("Documents inserted with IDs: %s", result.inserted_ids)
            else:
                result = self._coll.insert_one(docs)
                logger.info("Document inserted with ID: %s", result.inserted_id)
            return result
        except errors.PyMongoError as e:
            logger.error(f"Failed to send docs to MongoDB: {str(e)}")
            raise