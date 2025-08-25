import random
from typing import Dict, List
from sklearn.datasets import fetch_20newsgroups

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

class DataAnalyzer:
    """
    DataAnalyzer loads the 20 Newsgroups dataset by category and samples one text per category.
    """

    def __init__(self, subset) -> None:
        """
        Initialize the analyzer.

        Args:
            subset: Which subset to use from the dataset: 'train', 'test', or 'all'.
        """
        self.subset = subset

    def _one_text_from_category(self, category: str) -> str:
        """
        Return a single text sampled from a specific category.

        Args:
            category: The dataset category to sample from.

        Returns:
            One text string (may be empty if none found).
        """
        dataset = fetch_20newsgroups(
            subset=self.subset,
            categories=[category],
            remove=("headers", "footers", "quotes"),
        )
        texts = dataset["data"] if "data" in dataset else []
        if not texts:
            return ""
        return random.choice(texts)

    def _group(self, categories: List[str]) -> List[Dict[str, str]]:
        """
        Build a list of dicts, one per category.

        Args:
            categories: List of category names.

        Returns:
            List of dicts with keys 'category' and 'text'.
        """
        out: List[Dict[str, str]] = []
        for cat in categories:
            out.append({"category": cat, "text": self._one_text_from_category(cat)})
        return out

    def sample_messages(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Sample messages for both groups: interesting and not_interesting.

        Returns:
            A dictionary with two lists of message dicts (10 per group).
        """
        return {
            "interesting": self._group(INTERESTING_CATEGORIES),
            "not_interesting": self._group(NOT_INTERESTING_CATEGORIES),
        }


# if __name__ == "__main__":
#     analyzer = DataAnalyzer(subset="train")
#     messages = analyzer.sample_messages()
#     print(messages)