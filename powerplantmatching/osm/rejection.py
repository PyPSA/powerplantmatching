import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .models import ElementType, RejectionReason

logger = logging.getLogger(__name__)


@dataclass
class RejectedElement:
    id: str
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
        self.rejected_elements: dict[str, list[RejectedElement]] = {}
        self.ids: set[str] = set()
        self.categories: set[str] = set()

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
        identification = f"{element_type.value}/{element_id}"
        rejected = RejectedElement(
            id=identification,
            element_id=element_id,
            element_type=element_type,
            reason=reason,
            details=details,
        )

        # Add to main list
        if identification not in self.rejected_elements:
            self.rejected_elements[identification] = []
        self.rejected_elements[identification].append(rejected)

        # Add to id set
        self.ids.add(rejected.id)

        # Log rejection
        logger.debug(
            f"Rejected element {element_id} ({category}): {reason.value} - {details or ''}"
        )

    def get_rejection(self, id: str) -> list[RejectedElement] | None:
        """
        Get rejected elements by ID
        Parameters
        ----------
        id : str
            ID of the rejected element

        Returns
        -------
        list[RejectedElement] | None
            List of rejected elements with the given ID, or None if not found
        """
        return self.rejected_elements.get(id, None)

    def delete_rejection(self, id: str) -> bool:
        """
        Delete a rejected element by ID

        Parameters
        ----------
        id : str
            ID of the rejected element
        """
        success = False
        if id in self.rejected_elements:
            del self.rejected_elements[id]
            success = True
            logger.debug(f"Deleted rejection with ID: {id}")
        else:
            logger.debug(f"Rejection with ID {id} not found for deletion.")

        # Remove from ids
        if id in self.ids:
            self.ids.remove(id)

        return success

    def get_all_rejections(self) -> list[RejectedElement]:
        """
        Get all rejected elements

        Returns
        -------
        list[RejectedElement]
            list of all rejected elements
        """
        return self.rejected_elements

    def get_rejections_by_category(self, category: str) -> Iterator[RejectedElement]:
        """
        Get rejected elements for a specific category

        Parameters
        ----------
        category : str
            Category name

        Returns
        -------
        Iterator[RejectedElement]
            Iterator over rejected elements in the category
        """
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                if rejection.category == category:
                    yield rejection

    def get_summary(self) -> dict[str, dict[str, int]]:
        """
        Get a summary of rejection counts by category and reason

        Returns
        -------
        dict[str, dict[str, int]]
            Summary of rejections by category and reason
        """
        summary = {}
        for category in self.categories:
            summary[category] = {}
            for rejection in self.get_rejections_by_category(category):
                summary[category][rejection.reason.value] = (
                    summary[category].get(rejection.reason.value, 0) + 1
                )

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
        return len(self.get_rejections_by_category(category))

    def clear(self) -> None:
        """Clear all rejection data"""
        self.rejected_elements = {}
        self.categories = set()
        self.ids = set()
