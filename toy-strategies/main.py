from strategies.aapl_volume import AAPLVolumeStrategy
import os
from dotenv import load_dotenv

def main():

    load_dotenv()
    data_server_url = os.getenv('DATA_SERVER')
    strategy = AAPLVolumeStrategy()
    strategy.initialize(data_server_url)
    strategy.execute()
    
if __name__ == "__main__":
    main()