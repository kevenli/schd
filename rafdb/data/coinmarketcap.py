from datetime import datetime
from io import BytesIO
import os
import json
import time
from urllib.parse import urljoin
import pandas as pd
import requests
from .util import ensure_date
from .rtsdb import RTSDataService


class DataNotFound(FileNotFoundError):
    pass


class CoinMarketCapData(RTSDataService):
    def __init__(self, root_dir=None, cache_dir=None):
        if 'COINMARKETCAP_DATA_DIR' in os.environ and root_dir is None:
            root_dir = os.environ['COINMARKETCAP_DATA_DIR']

        if root_dir is None:
            root_dir = 'https://com-coinmarketcap.sgp1.digitaloceanspaces.com'

        super().__init__(root_dir=root_dir, cache_dir=cache_dir)

    def get_rankdata(self, date):
        date = ensure_date(date)
        filepath = f'rank/{date:%Y}/{date:%Y%m%d}/rank.json'

        json_data = json.load(self._read_file(filepath))

        columns = ['id', 'name', 'symbol', 'slug',
                   # num_market_pairs no data
                   'date_added', 'tags', 'max_supply', 
                   'circulating_supply', 'total_supply', 
                   # platform no data
                   'cmc_rank', 
                   # self_reported_circulating_supply no data.
                   # self_reported_market_cap no data.
                   # tvl_ratio no data.
                   'last_updated', 
                   # quotes in USD
                   'price', 'volume_24h', 'percent_change_1h', 'percent_change_24h', 
                   'percent_change_7d', 'market_cap']
        
        df = pd.DataFrame([[
            int(x['id']),
            x['name'],
            x['symbol'],
            x.get('slug'),
            datetime.strptime(x['date_added'], '%Y-%m-%dT%H:%M:%S.%fZ') if x.get('date_added') else None,
            x.get('tags',''),
            x.get('max_supply'),
            x.get('circulating_supply'),
            x['total_supply'],
            x.get('cmc_rank'),
            datetime.strptime(x['last_updated'], '%Y-%m-%dT%H:%M:%S.%fZ'),
            x['quote']['USD']['price'],
            x['quote']['USD']['volume_24h'],
            x['quote']['USD']['percent_change_1h'],
            x['quote']['USD']['percent_change_24h'],
            x['quote']['USD']['percent_change_7d'],
            x['quote']['USD']['market_cap'],
            ] for x in json_data['data']], columns=columns)

        return df
    
    def rank_file_v3(self, date):
        filepath = self.build_path('rank_file_v3', date, file_ext='.json')
        json_data = json.load(self._read_file(filepath))

        columns = ['id', 'name', 'symbol', 'slug',
                   # num_market_pairs no data
                   'date_added', 'tags', 'max_supply', 
                   'circulating_supply', 'total_supply', 
                   # platform no data
                   'cmc_rank', 
                   # self_reported_circulating_supply no data.
                   # self_reported_market_cap no data.
                   # tvl_ratio no data.
                   'last_updated', 
                   # quotes in USD
                   'price', 'volume_24h', 'percent_change_1h', 'percent_change_24h', 
                   'percent_change_7d', 'market_cap']
        
        df = pd.DataFrame([[
            int(x['id']),
            x['name'],
            x['symbol'],
            x.get('slug'),
            datetime.strptime(x['dateAdded'], '%Y-%m-%dT%H:%M:%S.%fZ') if x.get('dateAdded') else None,
            x.get('tags',''),
            x.get('maxSupply'),
            x.get('circulatingSupply'),
            x['totalSupply'],
            x.get('cmcRank'),
            datetime.strptime(x['lastUpdated'], '%Y-%m-%dT%H:%M:%S.%fZ'),
            x['quotes'][0]['price'],
            x['quotes'][0]['volume24h'],
            x['quotes'][0]['percentChange1h'],
            x['quotes'][0]['percentChange24h'],
            x['quotes'][0]['percentChange7d'],
            x['quotes'][0]['marketCap'],
            ] for x in json_data['data']], columns=columns)

        return df