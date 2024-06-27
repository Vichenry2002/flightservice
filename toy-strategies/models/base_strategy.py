from abc import ABC, abstractmethod
from typing import Dict, Optional, List
from pyarrow.flight import Ticket
from models.query import QDQuery

class BaseStrategy(ABC):
    """
    Abstract base class for strategies.
    Each strategy subclass must implement all abstract methods.
    """

    def __init__(self):
        # Initialize with default empty values that will be set in setup_strategy
        self.name = None
        self.query_to_tickets : Dict[QDQuery, List[Ticket]] = {}
        self.setup_strategy()

        # Validate that setup_strategy method has properly set name and queries
        if not self.name or not self.query_to_tickets:
            raise ValueError("Strategy name and queries must be defined in setup_strategy")

    @abstractmethod
    def setup_strategy(self):
        """
        Subclasses must implement this method to define their specific strategy name and queries.
        """
        pass

    @abstractmethod
    def initialize(self, data_server):
        """
        Initialize the strategy.

        This is where we will send flight descriptors to data server 
        (maybe obtain tickets for each flight desc).

        This allows the dataserver to prepare the data that will then be used in the execute method.
        """
        pass

    @abstractmethod
    def execute(self):
        """
        Execute the strategy's main functionality.
        This should contain the core logic of the strategy, such as making decisions,
        processing data, or executing trades.

        This method should be called after `initialize`.
        """
        pass


    def __str__(self):
        """
        Return a string representation of the strategy.
        """
        return f"Strategy Name: {self.name}\n Queries: {self.queries}"
