"""
market cap data from coinmarketcap 
"""

"""
Generate coinmarket rank csv from origin json file.
"""

import argparse
from datetime import datetime
import os
import json
import sys
import pkg_resources
import pandas as pd
from rafdb.data import coinmarketcap
from rafdb.data import binance as binancedata
from rafdb.data.binance import get_database as get_binance_database
from .contractinfo import get_contracts


def get_data_service() -> "coinmarketcap.CoinMarketCapData":
    """
    Get the data service object for CoinMarketCap data.
    """
    return coinmarketcap.CoinMarketCapData(cache_dir='.coinmarketcap_data')


def get_rank_data_from_file(filepath:str) -> "pd.DataFrame":
    """
    read data from file
    """
    with open(filepath, 'r') as f:
        json_data = json.load(f)

    columns = ['id', 'name', 'symbol', 'slug',
                   'date_added', 'tags', 'max_supply', 
                   'circulating_supply', 'total_supply', 
                   'cmc_rank', 
                   'last_updated', 
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


def try_parse_float_into_str(v):
    try:
        return str(int(v))
    except ValueError:
        return None


def get_rank_data(service:"coinmarketcap.CoinMarketCapData", date:"datetime") -> "pd.DataFrame":
    """
    columns:
        symbol: 
        date_added:
        max_supply:
        circulating_supply:
        total_supply:
        cmc_rank: int
        last_updated: datetime
        price: float
        volume_24h: 
        percent_change_1h
        percent_change_24h
        percent_change_7d
        marketCap
    """
    # old rankdata obsolete.
    # return service.get_rankdata(date)
    return service.rank_file_v3(date)


def main():
    parser = argparse.ArgumentParser(description='CoinMarketCap rank data')
    parser.add_argument('--date', '-d', default='20210101', help='date')
    parser.add_argument('--input')
    parser.add_argument('--output', '-o', help='output filepath')
    parser.add_argument('--force', '-f', action='store_true', default=False, help='force write data file')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='verbose output')
    args = parser.parse_args()

    verbose = args.verbose
    date = datetime.strptime(args.date, '%Y%m%d')
    output_filepath = args.output or f'output/marketcap/{date:%Y}/{date:%Y%m%d}/marketcap.csv'
    
    data_service = get_data_service()
    binance_data_service = get_binance_database()
    if args.input:
        df_origin_rankdata = get_rank_data_from_file(args.input)
    else:
        df_origin_rankdata = get_rank_data(data_service, date)
        # convert id into str type
        df_origin_rankdata['id'] = df_origin_rankdata['id'].apply(lambda v:str(int(v)))

    # tags is a list of tags, convert to space separated string.
    df_origin_rankdata['tags'] = df_origin_rankdata['tags'].apply(' '.join)

    data_path = pkg_resources.resource_filename('rafdb', 'csvdata/binance_futures_symbol_to_ucid.csv')
    df_future_symbol_ucid = pd.read_csv(data_path)

    df_contracts = get_contracts(binance_data_service, date)
    # df_contracts = df_contracts.merge(df_coin_ids[['id', 'baseAsset']], on='baseAsset', how='left')
    df_contracts = df_contracts.merge(df_future_symbol_ucid.rename(columns={'UCID':'id'}), on='symbol', how='left', validate='one_to_one')
    df_contracts['id'] = df_contracts['id'].apply(try_parse_float_into_str)

    # df_rankdata = df_rankdata.drop(columns=['symbol']).merge(df_contracts[['id', 'symbol']], on='id', how='left')
    df_rankdata = df_contracts[['id', 'symbol']].merge(df_origin_rankdata.drop(columns=['symbol']), on='id', how='left')

    df_rankdata_missing_id = df_rankdata.query('id != id')
    if not df_rankdata_missing_id.empty:
        print('symbols whose UCID not found: %s', df_rankdata_missing_id['symbol'])

    df_rankdata['date'] = date
    columns = ['symbol', 'date', 'date_added', 'max_supply', 'circulating_supply', 'total_supply',
               'cmc_rank', 'last_updated','price', 'volume_24h', 'percent_change_1h', 
               'percent_change_24h', 'percent_change_7d', 'market_cap', 'id']
    df_rankdata.sort_values('symbol', inplace=True)
    df_rankdata = df_rankdata[columns]

    if verbose:
        print(df_rankdata)

    if os.path.exists(output_filepath) and not args.force:
        print(f'File {output_filepath} already exists. Use -f to overwrite.')
        sys.exit(1)

    output_dir = os.path.dirname(output_filepath)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df_rankdata.to_csv(output_filepath, index=False)
    print(f'{len(df_rankdata)} rows written to {output_filepath}')


if __name__ == '__main__':
    main()

"""
python -m rafdb.marketcap --date=20220101
python -m rafdb.marketcap --date=20250115 --input=rank_file_v3_20250114.json
"""