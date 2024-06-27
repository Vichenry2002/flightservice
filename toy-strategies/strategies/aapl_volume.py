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
        Define the specific queries for the AAPL volume monitoring strategy.
        """
        self.name = "Apple Volume Monitoring"
    
        live_query = QDQuery(
            datasource="HUB_DAILY_DS",
            instruments=[InstrumentType.APPLE_INC, InstrumentType.ALPHABET_INC],  # Using the enum here
            indicators=[Indicators.VOLUME, Indicators.PRICE],  # Using the enum here
            start_date=datetime.now(),
            end_date=datetime.strptime("2024-12-31", "%Y-%m-%d") 
        )
        self.query_to_tickets[live_query] = []

    def initialize(self, data_server):
        #Upon initialization, a strategy defines a static, configurable set of queries it relies on. This "warns" the dataserver of
        #what data to prepare for.

        print(f"Initializing {self.name} with data server.")
        
        client = pa_f.FlightClient(location=data_server)
        
        for query in self.query_to_tickets.keys():
            try:
                info = client.get_flight_info(query.descriptor)
                tickets = [endpoint.ticket for endpoint in info.endpoints]
                self.query_to_tickets[query] = tickets
                print(f"Retrieved tickets for descriptor: {query.descriptor}")
            except Exception as e:
                print(f"Failed to retrieve tickets: {e}")
                self.query_to_tickets[query] = []
        

    def execute(self, data_server):
        #This is where the strategy will use the tickets to actually get record batches with data needed
        #This toy strategy simply prints the record batches it requested
        print(f"Executing {self.name}.")
        client = pa_f.FlightClient(location=data_server)  # Specify your data server location

        for query, tickets in self.query_to_tickets.items():
            print(f"Processing query: {query}")
            for ticket in tickets:
                try:
                    print(f"Fetching data for ticket: {ticket}")
                    reader = client.do_get(ticket)
                    batches = reader.read_all()
                    print(batches)  # Display the fetched record batches
                except Exception as e:
                    print(f"Error fetching data for ticket {ticket}: {e}")

        

    def __str__(self):
        return super().__str__()
