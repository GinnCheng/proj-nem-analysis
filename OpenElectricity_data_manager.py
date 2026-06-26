from openelectricity import OEClient, MarketMetric, DataMetric
from dotenv import load_dotenv
import pandas as pd
load_dotenv()

class OpenElectricityDataManager:
    def __init__(self, network_code: str, start_time: str, end_time: str, network_2nd_grp: str = None, interval: str = '5m'):
        self.network_code = network_code
        self.network_2nd_grp = network_2nd_grp
        self.client = OEClient()
        self.fetch_start = pd.to_datetime(start_time).tz_localize('Australia/Sydney').tz_convert('UTC').tz_localize(
            None).to_pydatetime()
        self.fetch_end = pd.to_datetime(end_time).tz_localize('Australia/Sydney').tz_convert('UTC').tz_localize(
            None).to_pydatetime()
        self.interval = interval

    def fetch_market_data(self):
        print(f"Fetching market data for {self.network_code} from {self.fetch_start} to {self.fetch_end}")
        market_data = self.client.get_market(
            network_code='NEM',
            metrics=[
                MarketMetric.PRICE,
                MarketMetric.DEMAND,
                MarketMetric.DEMAND_GROSS,
                MarketMetric.FLOW_IMPORTS,
                MarketMetric.FLOW_EXPORTS
            ],
            date_start=self.fetch_start,
            date_end=self.fetch_end,
            interval=self.interval
        ).to_pandas().groupby("interval", as_index=False).agg("sum")
        print(f"finished fetching market data for {self.network_code} from {self.fetch_start} to {self.fetch_end}")
        return market_data

    def fetch_network_data(self):
        print(f"Fetching network data for {self.network_code} from {self.fetch_start} to {self.fetch_end}")
        agg_grp = [ele for ele in ['interval'] + [self.network_2nd_grp] if ele is not None]
        network_data = self.client.get_network_data(
            network_code='NEM',
            metrics=[
                DataMetric.ENERGY,
                DataMetric.MARKET_VALUE,
                DataMetric.POWER,
                DataMetric.STORAGE_BATTERY,
            ],
            date_start=self.fetch_start,
            date_end=self.fetch_end,
            interval=self.interval,
            secondary_grouping=self.network_2nd_grp,
        ).to_pandas().groupby(agg_grp, as_index=False).agg("sum")
        print(f"finished fetching network data for {self.network_code} from {self.fetch_start} to {self.fetch_end}")
        return network_data