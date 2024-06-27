from strategies.aapl_volume import AAPLVolumeStrategy
import os
from dotenv import load_dotenv

def main():

    load_dotenv()
    data_server_url = os.getenv('DATA_SERVER')
    #testing strategy AAPLVolumeStrategy.
    strategy = AAPLVolumeStrategy()
    strategy.initialize(data_server_url)
    strategy.execute(data_server_url)
    
if __name__ == "__main__":
    main()