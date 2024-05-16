import datetime
from typing import Mapping, Sequence, TypeVar

from pyarrow.flight import FlightDescriptor
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator
from .types import InstrumentType, Indicators

T = TypeVar("T", bound="QDQuery")


class QDQuery(BaseModel):
    model_config = ConfigDict(frozen=True)

    datasource: str
    instruments: Sequence[InstrumentType]
    indicators: Sequence[Indicators]
    start_date: datetime.datetime = datetime.datetime.now()
    end_date: datetime.datetime 

    @field_validator("instruments", "indicators")
    @classmethod
    def sequence_to_frozenset(cls, v: Sequence[T]):
        return frozenset(e for e in v)

    @property
    def descriptor(self) -> FlightDescriptor:
        return FlightDescriptor.for_command(self.model_dump_json())
    
    def __hash__(self):
        # Hash method that uses a tuple of all important fields
        return hash((frozenset(self.instruments), frozenset(self.indicators), self.start_date, self.end_date))
    
    def __eq__(self, other):
        if not isinstance(other, QDQuery):
            return NotImplemented
        return (self.instruments == other.instruments and
                self.indicators == other.indicators and
                self.start_date == other.start_date and
                self.end_date == other.end_date)

class Spec(BaseModel):
    history: Mapping[str, QDQuery] = Field(default={})
    live: Mapping[str, QDQuery] = Field(default={})
