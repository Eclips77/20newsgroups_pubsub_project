import random
from typing import Dict, List, Optional, Literal
from sklearn.datasets import fetch_20newsgroups
from . import config
import logging

logger = logging.getLogger(__name__)


class DataAnalyzer:
    """
    DataAnalyzer loads the 20 Newsgroups dataset by category and samples one text per category.
    Follows Single Responsibility Principle - only responsible for data analysis and sampling.
    """

    def __init__(self, subset: Literal['train', 'test', 'all']) -> None:
        """
        Initialize the analyzer.

        Args:
            subset: Which subset to use from the dataset: 'train', 'test', or 'all'.
            
        Raises:
            ValueError: If subset is invalid
        """
        valid_subsets = ['train', 'test', 'all']
        if subset not in valid_subsets:
            raise ValueError(f"Invalid subset '{subset}'. Must be one of: {valid_subsets}")
            
        self.subset = subset
        logger.info("DataAnalyzer initialized with subset: %s", subset)

    def _one_text_from_category(self, category: str) -> str:
        """
        Return a single text sampled from a specific category.

        Args:
            category: The dataset category to sample from.

        Returns:
            str: One text string (may be empty if none found).
        """
        if not category or not isinstance(category, str):
            logger.warning("Invalid category provided: %s", category)
            return ""
            
        logger.debug("Fetching text from category: %s", category)
        
        try:
            dataset = fetch_20newsgroups(
                subset=self.subset,
                categories=[category],
                remove=("headers", "footers", "quotes"),
            )
            
            # Handle the Bunch object returned by sklearn
            texts = []
            try:
                texts = getattr(dataset, 'data', [])
            except Exception:
                logger.warning("Could not access 'data' attribute from dataset for category: %s", category)
                return ""
            
            logger.debug("Fetched %d texts from category '%s'", len(texts), category)
            
            if not texts:
                logger.warning("No texts found for category: %s", category)
                return ""
                
            selected_text = random.choice(texts)
            text_length = len(selected_text) if selected_text else 0
            logger.debug("Selected text from category %s, length: %d characters", category, text_length)
            
            return selected_text if selected_text else ""
            
        except Exception as e:
            logger.error("Error fetching text from category '%s': %s", category, str(e), exc_info=True)
            return ""

    def _group(self, categories: List[str]) -> List[Dict[str, str]]:
        """
        Build a list of dicts, one per category.

        Args:
            categories: List of category names.

        Returns:
            List[Dict[str, str]]: List of dicts with keys 'category' and 'text'.
            
        Raises:
            ValueError: If categories is empty or invalid
        """
        if not categories:
            raise ValueError("Categories list cannot be empty")
            
        if not isinstance(categories, list):
            raise ValueError("Categories must be a list")
            
        logger.info("Processing %d categories: %s", len(categories), categories)
        
        out: List[Dict[str, str]] = []
        successful_categories = 0
        
        for category in categories:
            try:
                text = self._one_text_from_category(category)
                message = {"category": category, "text": text}
                out.append(message)
                
                if text:  # Only count as successful if we got actual text
                    successful_categories += 1
                    
                logger.debug("Added message for category %s (text length: %d)", 
                           category, len(text))
                           
            except Exception as e:
                logger.error("Error processing category '%s': %s", category, str(e))
                # Still add an entry with empty text to maintain consistency
                out.append({"category": category, "text": ""})
        
        logger.info("Successfully processed %d/%d categories with text", 
                   successful_categories, len(categories))
        return out

    def sample_messages(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Sample messages for both groups: interesting and not_interesting.

        Returns:
            Dict[str, List[Dict[str, str]]]: A dictionary with two lists of message dicts.
            
        Raises:
            Exception: If sampling fails for both groups
        """
        logger.info("Starting message sampling process")
        
        try:
            interesting_messages = self._group(config.INTERESTING_CATEGORIES)
            not_interesting_messages = self._group(config.NOT_INTERESTING_CATEGORIES)
            
            result = {
                "interesting": interesting_messages,
                "not_interesting": not_interesting_messages,
            }
            
            # Validate that we got some data
            total_interesting = len([msg for msg in interesting_messages if msg.get("text")])
            total_not_interesting = len([msg for msg in not_interesting_messages if msg.get("text")])
            
            if total_interesting == 0 and total_not_interesting == 0:
                raise ValueError("No messages with text content were sampled")
            
            logger.info("Successfully sampled %d interesting and %d not_interesting messages with content", 
                       total_interesting, total_not_interesting)
            
            return result
            
        except Exception as e:
            logger.error("Error during message sampling: %s", str(e), exc_info=True)
            raise


# if __name__ == "__main__":
#     analyzer = DataAnalyzer(subset="train")
#     messages = analyzer.sample_messages()
#     print(messages["interesting"][0])