from math import log
import random
from typing import Dict, List
from sklearn.datasets import fetch_20newsgroups
from . import config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        logger.info("Fetched %d texts from category '%s'", len(texts), category)
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
        logger.info("Sampled %d messages for categories: %s", len(out), categories)
        return out

    def sample_messages(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Sample messages for both groups: interesting and not_interesting.

        Returns:
            A dictionary with two lists of message dicts (10 per group).
        """
        return {
            "interesting": self._group(config.INTERESTING_CATEGORIES),
            "not_interesting": self._group(config.NOT_INTERESTING_CATEGORIES),
        }


# if __name__ == "__main__":
#     analyzer = DataAnalyzer(subset="train")
#     messages = analyzer.sample_messages()
#     print(messages["interesting"][0])