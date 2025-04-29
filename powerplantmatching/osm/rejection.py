"""
Rejection tracking system for OSM data processing
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .models import ElementType, RejectionReason

logger = logging.getLogger(__name__)


@dataclass
class RejectedElement:
    element_id: str
    element_type: ElementType
    reason: RejectionReason
    details: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class RejectionTracker:
    """Centralized system for tracking and managing rejected elements"""

    def __init__(self):
        """Initialize the rejection tracker"""
        self.rejected_elements: list[RejectedElement] = []
        self.categories: dict[str, list[RejectedElement]] = {}

    def add_rejection(
        self,
        element_id: str,
        element_type: ElementType,
        reason: RejectionReason,
        details: Optional[str] = None,
        category: str = "default",
    ) -> None:
        """
        Add a rejected element to the tracker

        Parameters
        ----------
        element_id : str
            Identifier for the element
        element_type : ElementType
            Type of the element
        reason : RejectionReason
            Reason for rejection
        details : Optional[str]
            Additional details about the rejection
        category : str
            Category for grouping rejections (e.g., 'plant', 'generator')
        """
        rejected = RejectedElement(
            element_id=element_id,
            element_type=element_type,
            reason=reason,
            details=details,
        )

        # Add to main list
        self.rejected_elements.append(rejected)

        # Add to category
        if category not in self.categories:
            self.categories[category] = []
        self.categories[category].append(rejected)

        # Log rejection
        logger.debug(
            f"Rejected element {element_id} ({category}): {reason.value} - {details or ''}"
        )

    def get_all_rejections(self) -> list[RejectedElement]:
        """
        Get all rejected elements

        Returns
        -------
        list[RejectedElement]
            list of all rejected elements
        """
        return self.rejected_elements

    def get_category_rejections(self, category: str) -> list[RejectedElement]:
        """
        Get rejected elements for a specific category

        Parameters
        ----------
        category : str
            Category name

        Returns
        -------
        list[RejectedElement]
            list of rejected elements in the category
        """
        return self.categories.get(category, [])

    def get_summary(self) -> dict[str, dict[str, int]]:
        """
        Get a summary of rejection counts by category and reason

        Returns
        -------
        dict[str, dict[str, int]]
            Summary of rejections by category and reason
        """
        summary = {}

        # Add summary for each category
        for category, rejections in self.categories.items():
            category_summary = {}
            for rejection in rejections:
                reason = rejection.reason.value
                category_summary[reason] = category_summary.get(reason, 0) + 1
            summary[category] = category_summary

        return summary

    def get_total_count(self) -> int:
        """
        Get total count of rejections

        Returns
        -------
        int
            Total number of rejections
        """
        return len(self.rejected_elements)

    def get_category_count(self, category: str) -> int:
        """
        Get count of rejections for a specific category

        Parameters
        ----------
        category : str
            Category name

        Returns
        -------
        int
            Number of rejections in the category
        """
        return len(self.categories.get(category, []))

    def clear(self) -> None:
        """Clear all rejection data"""
        self.rejected_elements = []
        self.categories = {}

    def clear_category(self, category: str) -> None:
        """
        Clear rejection data for a specific category

        Parameters
        ----------
        category : str
            Category to clear
        """
        if category in self.categories:
            # Remove from main list
            self.rejected_elements = [
                r for r in self.rejected_elements if r not in self.categories[category]
            ]
            # Remove category
            del self.categories[category]
