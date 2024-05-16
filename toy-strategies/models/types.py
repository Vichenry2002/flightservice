from enum import Enum

class InstrumentType(Enum):
    APPLE_INC = "AAPL"
    MICROSOFT_CORPORATION = "MSFT"
    ALPHABET_INC = "GOOGL"
    AMAZON_COM_INC = "AMZN"
    META_PLATFORMS_INC = "FB"
    BERKSHIRE_HATHAWAY_INC = "BRK.B"
    JOHNSON_AND_JOHNSON = "JNJ"
    VISA_INC = "V"
    PROCTER_AND_GAMBLE_CO = "PG"
    TESLA_INC = "TSLA"

class Indicators(Enum):
    VOLUME = "volume"
    PRICE = "price"
