from typing import Dict, Any
from fastapi import FastAPI
from . import config
from .data_analyzer import DataAnalyzer
from .producer import publish_group

app = FastAPI(title="Publisher Service", version="1.0.0")

@app.get("/publish")
def publish_messages() -> Dict[str, Any]:
    """
    Fetch and publish 20 messages (10 per group) to Kafka.

    Returns:
        JSON with number of messages published per topic.
    """
    analyzer = DataAnalyzer(subset=config.DATASET_SUBSET)
    data = analyzer.sample_messages()

    sent_interesting = publish_group(data["interesting"], config.KAFKA_TOPIC_INTERESTING)
    sent_not_interesting = publish_group(data["not_interesting"], config.KAFKA_TOPIC_NOT_INTERESTING)

    return {"published": {"interesting": sent_interesting, "not_interesting": sent_not_interesting}}
