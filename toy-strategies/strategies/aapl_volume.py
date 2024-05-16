from models.base_strategy import BaseStrategy
from models.query import QDQuery,Spec
from models.types import Indicators, InstrumentType
from datetime import datetime
from typing import Dict, Optional
from pyarrow.flight import Ticket
import pyarrow.flight as pa_f
import pyarrow as pa


class AAPLVolumeStrategy(BaseStrategy):
    def setup_strategy(self):
        """
        Define the specific settings for the AAPL volume monitoring strategy.
        """
        self.name = "Apple Volume Monitoring"
    
        live_query = QDQuery(
            datasource="HUB_DAILY_DS",
            instruments=[InstrumentType.APPLE_INC],  # Using the enum here
            indicators=[Indicators.VOLUME],  # Using the enum here
            start_date=datetime.now(),
            end_date=datetime.strptime("2024-12-31", "%Y-%m-%d") 
        )
        self.query_to_tickets[live_query] = []

    def initialize(self, data_server):
        print(f"Initializing {self.name} with data server.")
        
        client = pa_f.FlightClient(location=data_server)
        
        #TODO: Test this
        for query in self.query_to_tickets:
            descriptor = query.descriptor
            info = client.get_flight_info(descriptor)
            tickets = []
            for endpoint in info.endpoints:
                tickets.append(endpoint.ticket)
            self.query_to_tickets[query] = [tickets]
            print(descriptor)
        


    def execute(self):
        print(f"Executing {self.name}.")
        #This is where the strategy will use the tickets to actually get record batches with data needed
        

    def __str__(self):
        return super().__str__()
