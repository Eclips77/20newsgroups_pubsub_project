import os
from typing import List


KAFKA_BOOTSTRAP: List[str] = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092").split(",")
KAFKA_TOPIC_INTERESTING: str = os.getenv("KAFKA_TOPIC_INTERESTING", "interesting")
KAFKA_TOPIC_NOT_INTERESTING: str = os.getenv("KAFKA_TOPIC_NOT_INTERESTING", "not_interesting")
DATASET_SUBSET: str = os.getenv("DATASET_SUBSET", "train")

INTERESTING_CATEGORIES = [
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
NOT_INTERESTING_CATEGORIES = [
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

