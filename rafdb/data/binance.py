from datetime import datetime, timedelta, date
from io import BytesIO
import itertools
import os.path
import json
import logging
import time
from urllib.parse import urljoin
import pandas as pd
import requests
from .util import iter_dates, ensure_date

logger = logging.getLogger(__name__)


class DataNotFound(Exception):
    pass


class BinanceData:
    def __init__(self, root_dir=None, cache_dir=None, delay:float=0.1):
        if 'BINANCE_DATA_ROOT' in os.environ and root_dir is None:
            root_dir = os.environ['BINANCE_DATA_ROOT']

        if root_dir is None:
            root_dir = 'https://com-binance.sgp1.digitaloceanspaces.com/'
            
        self.root_dir = root_dir

        if cache_dir is None and 'BINANCE_CACHE_DIR' in os.environ:
            cache_dir = os.environ['BINANCE_CACHE_DIR']

        if cache_dir is None:
            cache_dir = '.binance_cache'

        self.cache_dir = cache_dir
        self._session = requests.session()
        self._delay = delay or 0.1

    def _read_file(self, path) -> "BytesIO":
        if self.cache_dir and os.path.exists(os.path.join(self.cache_dir, path)):
            return open(os.path.join(self.cache_dir, path), 'r', encoding='utf8')

        if self.root_dir.startswith('http'):
            url = urljoin(self.root_dir, path)
            # res = requests.get(url, timeout=30)
            res = self._session.get(url, timeout=30)
            try:
                res.raise_for_status()
                time.sleep(self._delay)
                if self.cache_dir:
                    cache_filepath = os.path.join(self.cache_dir, path)
                    os.makedirs(os.path.dirname(cache_filepath), exist_ok=True)
                    with open(cache_filepath, 'w', encoding='utf8') as f:
                        f.write(res.text)

                return BytesIO(res.content)
            except requests.exceptions.HTTPError as ex:
                if ex.response.status_code in (403, 404):
                    raise DataNotFound() from ex
                raise
        else:
            try:
                filepath = os.path.join(self.root_dir, path)
                return open(filepath, 'r', encoding='utf8')
            except FileNotFoundError as exc:
                raise DataNotFound(filepath) from exc

    def _build_filepath(self, datatype, relative_path):
        return os.path.join(self.root_dir, datatype, relative_path)
    
    def _build_fapi_kline_1d_path(self, symbol, date):
        pass

    def get_fapi_kline_1d(self, symbol, date):
        date = ensure_date(date)
        filepath = f'fapi_kline_1d/{date:%Y}/{date:%Y%m%d}/{symbol}.json'

        try:
            json_data = json.load(self._read_file(filepath))
        except FileNotFoundError:
            raise DataNotFound()

        df = pd.DataFrame([[datetime.utcfromtimestamp(x[0]/1000), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), datetime.utcfromtimestamp(x[6]/1000), float(x[7]), x[8], float(x[9])] for x in json_data], columns=['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'Turnover', 'NumberOfTrades', 'TakerBuyVolume'])

        return df

    def get_fapi_kline_1m(self, symbol, date):
        date = ensure_date(date)
        filepath = f'fapi_kline_1m/{date:%Y}/{date:%Y%m%d}/{symbol}.json'
        try:
            json_data = json.load(self._read_file(filepath))
        except json.decoder.JSONDecodeError:
            raise Exception('Empty data', filepath)

        df = pd.DataFrame([[datetime.utcfromtimestamp(x[0]/1000), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), datetime.utcfromtimestamp(x[6]/1000), float(x[7]), x[8], float(x[9])] for x in json_data], columns=['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'Turnover', 'NumberOfTrades', 'TakerBuyVolume'])

        return df
    
    def get_fapi_funding_rate(self, symbol, date):
        """
        fields:
            fundingTime: int
            symbol: str
            fundingRate: str
            markPrice: str
        """
        date = ensure_date(date)
        filepath = f'fapi_fundingrate/{date:%Y}/{date:%Y%m%d}/{symbol}.json'
        try:
            json_data = json.load(self._read_file(filepath))
        except json.decoder.JSONDecodeError:
            raise Exception('Empty data', filepath)
        df = pd.DataFrame(json_data)
        return df

    def get_fapi_exchange_info_symbols(self, date):
        date = ensure_date(date)

        filepath = f'fapi_exchangeinfo/{date:%Y}/{date:%Y%m%d}/fapi_exchangeinfo.json'
            
        json_data = json.load(self._read_file(filepath))

        columns = ['symbol', 'pair', 'contractType', 'deliveryDate', 'onboardDate', 'status', 'maintMarginPercent', 'requiredMarginPercent', 
                   'baseAsset', 'quoteAsset', 'marginAsset', 'pricePrecision', 'quantityPrecision', 'baseAssetPrecision', 'quotePrecision', 
                   'underlyingType', 'settlePlan', 'triggerProtect', 'liquidationFee', 'marketTakeBound', 'lotSize']

        def extract_filter_item(symbol_info, filter_type, field):
            filters = symbol_info['filters']
            try:
                filter_item = next(filter(lambda x: x['filterType'] == filter_type, filters))
            except StopIteration:
                return None
            
            return filter_item.get(field)

        def extractRow(x):
            try:
                step_size = float(next(filter(lambda i: i['filterType'] == 'LOT_SIZE', x['filters']))['stepSize'])
            except StopIteration:
                step_size = None

            min_lot_size = extract_filter_item(x, 'LOT_SIZE', 'minQty')
            step_lot_size = extract_filter_item(x, 'LOT_SIZE', 'stepSize')

            return [
                x['symbol'],
                x['pair'],
                x['contractType'],
                datetime.utcfromtimestamp(x['deliveryDate']/1000),
                datetime.utcfromtimestamp(x['onboardDate']/1000),
                x['status'],
                float(x['maintMarginPercent']),
                float(x['requiredMarginPercent']),
                x['baseAsset'],
                x['quoteAsset'],
                x['marginAsset'],
                x['pricePrecision'],
                x['quantityPrecision'],
                x['baseAssetPrecision'],
                x['quotePrecision'],
                x['underlyingType'],
                x.get('settlePlan'), # can be null
                float(x['triggerProtect']),
                float(x['liquidationFee']),
                float(x['marketTakeBound']),
                min_lot_size,
            ]

        df = pd.DataFrame([extractRow(x) for x in json_data['symbols']], columns=columns)

        return df

    def get_fapi_kline_1m_ts(self, symbol, startdate, enddate):
        startdate = ensure_date(startdate)
        enddate = ensure_date(enddate)
        if enddate < startdate:
            raise Exception("enddate cannot be earlier than startdate")

        d = startdate
        dfs = []
        while d <= enddate:
            dfs.append(self.get_fapi_kline_1m(symbol, d))
            d += timedelta(days=1)

        return pd.concat(dfs)

    def get_fapi_kline_1d_ts(self, symbol, startdate, enddate):
        startdate = ensure_date(startdate)
        enddate = ensure_date(enddate)
        if enddate < startdate:
            raise Exception("enddate cannot be earlier than startdate")

        d = startdate
        dfs = []
        while d <= enddate:
            dfs.append(self.get_fapi_kline_1d(symbol, d))
            d += timedelta(days=1)

        return pd.concat(dfs)
    
    def get_fapi_openinterest_1d(self, symbol:str, end_date:"datetime"):
        end_date = ensure_date(end_date)
        filepath = f'fapi_openinterest_1d/{end_date:%Y}/{end_date:%Y%m%d}/{symbol}.json'
        try:
            json_data = json.load(self._read_file(filepath))
        except FileNotFoundError:
            raise DataNotFound()

        df = pd.DataFrame(json_data, columns=['symbol', 'sumOpenInterest', 'sumOpenInterestValue', 'timestamp'])
        df['sumOpenInterest'] = df['sumOpenInterest'].astype(float)
        df['sumOpenInterestValue'] = df['sumOpenInterestValue'].astype(float)
        df['datatime'] = df['timestamp'].apply(lambda x: datetime.utcfromtimestamp(x/1000))
        return df
    
    def get_dapi_kline_1d(self, symbol, date):
        date = ensure_date(date)
        filepath =f'dapi_kline_1d/{date:%Y}/{date:%Y%m%d}/{symbol}.json'
        try:
            json_data = json.load(self._read_file(filepath))
        except FileNotFoundError:
            raise DataNotFound()

        df = pd.DataFrame([[datetime.utcfromtimestamp(x[0]/1000), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), datetime.utcfromtimestamp(x[6]/1000), float(x[7]), x[8], float(x[9])] for x in json_data], columns=['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'Turnover', 'NumberOfTrades', 'TakerBuyVolume'])

        return df

    def get_dapi_kline_1m(self, symbol, date):
        date = ensure_date(date)
        filepath = f'dapi_kline_1m/{date:%Y}/{date:%Y%m%d}/{symbol}.json'
        try:
            json_data = json.load(self._read_file(filepath))
        except FileNotFoundError:
            raise DataNotFound()

        df = pd.DataFrame([[datetime.utcfromtimestamp(x[0]/1000), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), datetime.utcfromtimestamp(x[6]/1000), float(x[7]), x[8], float(x[9])] for x in json_data], columns=['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'Turnover', 'NumberOfTrades', 'TakerBuyVolume'])

        return df
    
    def get_dapi_kline_1m_ts(self, symbol, start_date, end_date=None):
        if end_date is None:
            end_date = start_date

        start_date = ensure_date(start_date)
        end_date = ensure_date(end_date)

        dfs = []
        for d in iter_dates(start_date, end_date):
            df= self.get_dapi_kline_1m(symbol, d)
            dfs.append(df)

        return pd.concat(dfs)
    
    def get_dapi_cont_kline_1m(self, pair, contract_type, date):
        date = ensure_date(date)
        if contract_type not in ('PERPETUAL', 'CURRENT_QUARTER', 'NEXT_QUARTER'):
            raise Exception("Invalid contract_type")
        
        filepath = f'dapi_cont_kline_1m/{date:%Y}/{date:%Y%m%d}/{pair}_{contract_type}.json'
        try:
            json_data = json.load(self._read_file(filepath))
        except FileNotFoundError:
            raise DataNotFound(filepath)
        except json.JSONDecodeError:
            raise Exception("Wrong content type, origin filepath is %s" % filepath)

        df = pd.DataFrame([[datetime.utcfromtimestamp(x[0]/1000), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), datetime.utcfromtimestamp(x[6]/1000), float(x[7]), x[8], float(x[9])] for x in json_data], columns=['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'Turnover', 'NumberOfTrades', 'TakerBuyVolume'])

        return df
    
    def get_dapi_cont_kline_1m_ts(self, pair, contract_type, start_date, end_date=None):
        if end_date is None:
            end_date = start_date

        dfs = []
        for d in iter_dates(start_date, end_date):
            df= self.get_dapi_cont_kline_1m(pair, contract_type, d)
            dfs.append(df)

        return pd.concat(dfs)

    def get_dapi_cont_kline_1d(self, pair, contract_type, date):
        date = ensure_date(date)
        if contract_type not in ('PERPETUAL', 'CURRENT_QUARTER', 'NEXT_QUARTER'):
            raise Exception("Invalid contract_type")
        
        filepath = f'dapi_cont_kline_1d/{date:%Y}/{date:%Y%m%d}/{pair}_{contract_type}.json'
        try:
            json_data = json.load(self._read_file(filepath))
        except FileNotFoundError:
            raise DataNotFound()

        df = pd.DataFrame([[datetime.utcfromtimestamp(x[0]/1000), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), datetime.utcfromtimestamp(x[6]/1000), float(x[7]), x[8], float(x[9])] for x in json_data], columns=['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'Turnover', 'NumberOfTrades', 'TakerBuyVolume'])

        return df

    def get_dapi_cont_kline_1d_ts(self, pair, contract_type, start_date, end_date=None):
        if end_date is None:
            end_date = start_date

        dfs = []
        for d in iter_dates(start_date, end_date):
            df= self.get_dapi_cont_kline_1d(pair, contract_type, d)
            dfs.append(df)

        return pd.concat(dfs)

    def get_dapi_cont_kline_1h(self, pair, contract_type, date):
        date = ensure_date(date)
        if contract_type not in ('PERPETUAL', 'CURRENT_QUARTER', 'NEXT_QUARTER'):
            raise Exception("Invalid contract_type")
        
        filepath = f'dapi_cont_kline_1h/{date:%Y}/{date:%Y%m%d}/{pair}_{contract_type}.json'

        try:
            json_data = json.load(self._read_file(filepath))
        except FileNotFoundError as ex:
            raise DataNotFound() from ex

        df = pd.DataFrame([[datetime.utcfromtimestamp(x[0]/1000), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), datetime.utcfromtimestamp(x[6]/1000), float(x[7]), x[8], float(x[9])] for x in json_data], columns=['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'Turnover', 'NumberOfTrades', 'TakerBuyVolume'])

        return df
    
    def get_dapi_cont_kline_1h_ts(self, pair, contract_type, start_date, end_date=None):
        if end_date is None:
            end_date = start_date

        dfs = []
        for d in iter_dates(start_date, end_date):
            df= self.get_dapi_cont_kline_1h(pair, contract_type, d)
            dfs.append(df)

        return pd.concat(dfs)

    def get_dapi_exchange_info_symbols(self, date):
        date = ensure_date(date)

        filepath = f'dapi_exchangeinfo/{date:%Y}/{date:%Y%m%d}/dapi_exchangeinfo.json'
            
        try:
            json_data = json.load(self._read_file(filepath))
        except FileNotFoundError:
            raise DataNotFound()

        columns = ['symbol', 'pair', 'contractType', 'deliveryDate', 'onboardDate', 'contractStatus', 'maintMarginPercent', 'requiredMarginPercent', 
                   'baseAsset', 'quoteAsset', 'marginAsset', 'pricePrecision', 'quantityPrecision', 'baseAssetPrecision', 'quotePrecision', 
                   'underlyingType', 'triggerProtect', 'liquidationFee', 'marketTakeBound', 'lotSize']

        def extractRow(x):
            try:
                step_size = float(next(filter(lambda i: i['filterType'] == 'LOT_SIZE', x['filters']))['stepSize'])
            except StopIteration:
                step_size = None

            return [
                x['symbol'],
                x['pair'],
                x['contractType'],
                datetime.utcfromtimestamp(x['deliveryDate']/1000),
                datetime.utcfromtimestamp(x['onboardDate']/1000),
                x['contractStatus'],
                float(x['maintMarginPercent']),
                float(x['requiredMarginPercent']),
                x['baseAsset'],
                x['quoteAsset'],
                x['marginAsset'],
                x['pricePrecision'],
                x['quantityPrecision'],
                x['baseAssetPrecision'],
                x['quotePrecision'],
                x['underlyingType'],
                float(x['triggerProtect']),
                float(x['liquidationFee']),
                float(x['marketTakeBound']),
                step_size,
            ]

        df = pd.DataFrame([extractRow(x) for x in json_data['symbols']], columns=columns)

        return df

    def get_dapi_funding_rate(self, symbol, date):
        date = ensure_date(date)

        filepath = f'dapi_fundingrate/{date:%Y}/{date:%Y%m%d}/{symbol}.json'
            
        try:
            json_data = json.load(self._read_file(filepath))
        except FileNotFoundError:
            raise DataNotFound()

        df = pd.DataFrame(json_data)
        df['fundingTime'] = df['fundingTime'].apply(lambda x: datetime.utcfromtimestamp(x/1000))
        df['fundingRate'] = df['fundingRate'].astype(float)
        df.set_index('fundingTime', inplace=True)
        return df

    def get_dapi_funding_rate_ts(self, symbol, start_date, end_date):
        start_date = ensure_date(start_date)
        end_date = ensure_date(end_date)
        dfs = []
        d = start_date
        while d <= end_date:
            dfs.append(self.get_dapi_funding_rate(symbol, d))
            d += timedelta(days=1)

        return pd.concat(dfs)
    
    def get_dapi_open_interest(self, symbol, date):
        date = ensure_date(date)

        filepath = f'dapi_openinterest_1d/{date:%Y}/{date:%Y%m%d}/{symbol}.json'
        try:
            json_data = json.load(self._read_file(filepath))
        except FileNotFoundError as ex:
            raise DataNotFound(filepath) from ex

        df = pd.DataFrame(json_data, columns=['contractType', 'sumOpenInterest', 'sumOpenInterestValue', 'pair', 'timestamp'])
        df['sumOpenInterest'] = df['sumOpenInterest'].astype(float)
        df['sumOpenInterestValue'] = df['sumOpenInterestValue'].astype(float)
        df['datatime'] = df['timestamp'].apply(lambda x: datetime.utcfromtimestamp(x/1000))
        return df
    
    def get_dapi_open_interest_ts(self, symbol, start_date, end_date):
        start_date = ensure_date(start_date)
        end_date = ensure_date(end_date)
        dfs = []
        d = start_date
        while d <= end_date:
            dfs.append(self.get_dapi_open_interest(symbol, d))
            d += timedelta(days=1)

        return pd.concat(dfs)

    def get_spot_exchange_symbols(self, date):
        date = ensure_date(date)
        filepath = f'api_exchangeinfo/{date:%Y}/{date:%Y%m%d}.json'
        try:
            json_data = json.load(self._read_file(filepath))
        except FileNotFoundError:
            raise DataNotFound(filepath)

        columns = [
            'symbol', 
            'status', 
            'baseAsset', 
            'baseAssetPrecision', 
            'quoteAsset', 
            'quotePrecision', 
            'quoteAssetPrecision', 
            'baseCommissionPrecision', 
            'quoteCommissionPrecision', 
        ]

        def extractRow(x):
            return [
                x['symbol'], 
                x['status'], 
                x['baseAsset'], 
                x['baseAssetPrecision'], 
                x['quoteAsset'], 
                x['quotePrecision'], 
                x['quoteAssetPrecision'], 
                x['baseCommissionPrecision'], 
                x['quoteCommissionPrecision'], 
            ]

        df = pd.DataFrame([extractRow(x) for x in json_data['symbols']], columns=columns)

        return df

    def get_spot_kline_1d(self, symbol, date):
        date = ensure_date(date)

        filepath = f'api_klines_1d/{date:%Y}/{date:%Y%m%d}/{symbol}.json'
        try:
            json_data = json.load(self._read_file(filepath))
        except FileNotFoundError:
            raise DataNotFound()

        df = pd.DataFrame([[datetime.utcfromtimestamp(x[0]/1000), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), datetime.utcfromtimestamp(x[6]/1000), float(x[7]), x[8], float(x[9])] for x in json_data], columns=['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'Turnover', 'NumberOfTrades', 'TakerBuyVolume'])

        return df

    def get_spot_kline_1d_ts(self, symbol, start_date, end_date):
        start_date = ensure_date(start_date)
        end_date = ensure_date(end_date)
        if end_date < start_date:
            raise Exception("enddate cannot be earlier than startdate")

        d = start_date
        dfs = []
        while d <= end_date:
            dfs.append(self.get_spot_kline_1d(symbol, d))
            d += timedelta(days=1)

        return pd.concat(dfs)

    def get_spot_kline_1m(self, symbol, date):
        date = ensure_date(date)

        def read_single_file_data(symbol, date, no_of_day):
            # there are 2 files for each symbol each date
            # filepath as [api_klines_1m/2022/20220926/BTCUSDT_0.json api_klines_1m/2022/20220926/BTCUSDT_1.json]
            # read and return specified file as a list of list of field data.
            filepath = f'api_klines_1m/{date:%Y}/{date:%Y%m%d}/{symbol}_{no_of_day}.json'

            try:
                json_data = json.load(self._read_file(filepath))
            except FileNotFoundError:
                raise DataNotFound()
            
            return [[datetime.utcfromtimestamp(x[0]/1000), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), datetime.utcfromtimestamp(x[6]/1000), float(x[7]), x[8], float(x[9])] for x in json_data]

        # flatten rows 
        all_rows =tuple(itertools.chain.from_iterable([read_single_file_data(symbol, date, i) for i in [0,1]]))
        df = pd.DataFrame(all_rows, columns=['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'Turnover', 'NumberOfTrades', 'TakerBuyVolume'])

        return df

    def get_spot_kline_1m_ts(self, symbol, start_date, end_date):
        start_date = ensure_date(start_date)
        end_date = ensure_date(end_date)
        if end_date < start_date:
            raise Exception("enddate cannot be earlier than startdate")

        d = start_date
        dfs = []
        while d <= end_date:
            dfs.append(self.get_spot_kline_1m(symbol, d))
            d += timedelta(days=1)

        return pd.concat(dfs)

def get_database() -> BinanceData:
    root_dir = 'https://com-binance.sgp1.digitaloceanspaces.com/'

    cache_dir = None
    if 'BINANCE_CACHE_DIR' in os.environ:
        cache_dir = os.environ['BINANCE_CACHE_DIR']

    if 'BINANCE_DELAY' in os.environ:
        delay = float(os.environ['BINANCE_DELAY'])
    else:
        delay = 0.1
    
    return BinanceData(root_dir, cache_dir, delay)
