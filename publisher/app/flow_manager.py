from .producer import ProducerService
from .data_analyzer import DataAnalyzer
from . import config
from typing import Dict, Any



class FlowManager:
    def __init__(self) -> None:
        self._producer = ProducerService()
        self._data_analyzer = DataAnalyzer(subset=config.DATASET_SUBSET)
    

    def fetch_and_publish(self) -> Dict[str, Any]:
        """
        Fetches a sample of messages, categorizes them as 'interesting' or 'not interesting',
        and publishes each group to their respective Kafka topics.
        Returns:
            Dict[str, Any]: A dictionary containing the results of the publish operations for both
            'interesting' and 'not interesting' message groups.
        """

        data = self._data_analyzer.sample_messages()
        sent_interesting = self._producer.publish_group(
            data["interesting"], config.KAFKA_TOPIC_INTERESTING
        )
        sent_not_interesting = self._producer.publish_group(
            data["not_interesting"], config.KAFKA_TOPIC_NOT_INTERESTING
        )
        return {"published": {"interesting": sent_interesting, "not_interesting": sent_not_interesting}}
