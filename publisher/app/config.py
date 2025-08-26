import os
from typing import List


class PublisherConfig:
    """
    Minimal OOP wrapper for publisher configuration.
    Keeps module-level constants for backward compatibility.
    """

    def __init__(self) -> None:
        self.KAFKA_BOOTSTRAP: List[str] = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092").split(",")
        self.KAFKA_TOPIC_INTERESTING: str = os.getenv("KAFKA_TOPIC_INTERESTING", "interesting")
        self.KAFKA_TOPIC_NOT_INTERESTING: str = os.getenv("KAFKA_TOPIC_NOT_INTERESTING", "not_interesting")
        # Dataset subset can be: "train", "test", or "all"
        self.DATASET_SUBSET: str = os.getenv("DATASET_SUBSET", "train")
        self.INTERESTING_CATEGORIES = [
            "alt.atheism",
            "comp.graphics",
            "comp.os.ms-windows.misc",
            "comp.sys.ibm.pc.hardware",
            "comp.sys.mac.hardware",
            "comp.windows.x",
            "misc.forsale",
            "rec.autos",
            "rec.motorcycles",
            "rec.sport.baseball",
        ]
        self.NOT_INTERESTING_CATEGORIES = [
            "rec.sport.hockey",
            "sci.crypt",
            "sci.electronics",
            "sci.med",
            "sci.space",
            "soc.religion.christian",
            "talk.politics.guns",
            "talk.politics.mideast",
            "talk.politics.misc",
            "talk.religion.misc",
        ]


# Instantiate a default config and expose attributes for compatibility
_DEFAULT_CFG = PublisherConfig()

KAFKA_BOOTSTRAP: List[str] = _DEFAULT_CFG.KAFKA_BOOTSTRAP
KAFKA_TOPIC_INTERESTING: str = _DEFAULT_CFG.KAFKA_TOPIC_INTERESTING
KAFKA_TOPIC_NOT_INTERESTING: str = _DEFAULT_CFG.KAFKA_TOPIC_NOT_INTERESTING
DATASET_SUBSET: str = _DEFAULT_CFG.DATASET_SUBSET
INTERESTING_CATEGORIES = _DEFAULT_CFG.INTERESTING_CATEGORIES
NOT_INTERESTING_CATEGORIES = _DEFAULT_CFG.NOT_INTERESTING_CATEGORIES