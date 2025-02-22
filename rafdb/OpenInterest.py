"""
produce OpenInterest dataset.
  * symbol
  * date
  * sumOpenInterest
  * sumOpenInterestValue
"""
import argparse
from datetime import datetime
import os
import sys
import time
import pandas as pd
from rafdb.data.binance import BinanceData, DataNotFound
from rafdb.contractinfo import get_symbols

def main():
    parser = argparse.ArgumentParser(description='produce contractinfo data table')
    parser.add_argument('--date', required=True, type=str, help='date in YYYYMMDD format')
    # output parameter
    parser.add_argument('--output', '-o', type=str, help='output filepath')

    args = parser.parse_args()
    date = datetime.strptime(args.date, '%Y%m%d')
    output_filepath = args.output or f'output/OpenInterest/{date:%Y}/{date:%Y%m%d}/OpenInterest.csv'
    
    time_start = time.time()
    bd = BinanceData()

    dfs = []
    try:
        df_exchange_info = get_symbols(bd, date)
    except DataNotFound:
        # if exchange_info of the specified date not exists,
        # try again with the yesterday's exchange info
        # data_date = datetime.now() - pd.Timedelta(days=1)
        data_date = datetime.now()
        df_exchange_info = get_symbols(bd, data_date)

    df_exchange_info = df_exchange_info.query('deliveryDate > @date and onboardDate <= @date')
    symbols = df_exchange_info['symbol']

    for symbol in symbols:
        df_quote = bd.get_fapi_openinterest_1d(symbol, date)
        df_quote['symbol'] = symbol
        df_quote['date'] = date
        dfs.append(df_quote)

    df_openinterest = pd.concat(dfs, ignore_index=True)
    if df_openinterest.empty:
        print('no data', file=sys.stderr)
        sys.exit(1)

    df_openinterest = df_openinterest[['symbol', 'date', 'sumOpenInterest', 'sumOpenInterestValue']]

    print(df_openinterest)

    output_dir = os.path.dirname(output_filepath)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df_openinterest.to_csv(output_filepath, index=False)

    time_end = time.time()

    print(f'{len(df_openinterest)} rows written to {output_filepath}, {time_end-time_start:.3f}s')


if __name__ == '__main__':
    main()

"""
python -m rafdb.OpenInterest --date=20220101
"""