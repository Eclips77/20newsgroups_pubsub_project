"""
data_analyzer.py
----------------
Utility module that fetches and prepares messages from the 20 Newsgroups dataset.

The analyzer provides:
    - sample_messages() -> Dict[str, List[dict]]
      Returns a dictionary with two keys "interesting" and "not_interesting",
      each holding a list of 10 items (one per category). Each item is a dict:
          { "category": <str>, "text": <str> }

Design choices:
- Uses dataset["data"] (dict-style) to avoid attribute-style access like ds.data.
- Removes headers/footers/quotes for cleaner text.
- Returns empty string if no text is available for a category.
"""

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

    def __init__(self, subset: str = "train") -> None:
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
        ds = fetch_20newsgroups(
            subset=self.subset,
            categories=[category],
            remove=("headers", "footers", "quotes"),
        )
        texts = ds["data"] if "data" in ds else []
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
