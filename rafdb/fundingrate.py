"""
produce dailyquote data table.
"""
import argparse
from datetime import datetime, timedelta
from decimal import Decimal
import os
import sys
import time
import pandas as pd
from rafdb.data.binance import BinanceData, DataNotFound
from rafdb.data.binance import get_database as get_binance_database
from rafdb.contractinfo import get_symbols

def main():
    parser = argparse.ArgumentParser(description='produce fundingrate data table')
    parser.add_argument('--date', required=True, type=str, help='date in YYYYMMDD format')
    # output parameter
    parser.add_argument('--output', '-o', type=str, help='output filepath')

    args = parser.parse_args()
    date = datetime.strptime(args.date, '%Y%m%d')
    output_filepath = args.output or f'output/fundingrate/{date:%Y}/{date:%Y%m%d}/fundingrate.csv'
    
    time_start = time.time()
    bd = get_binance_database()

    dfs = []
    try:
        df_exchange_info = get_symbols(bd, date)
    except DataNotFound:
        # if exchange_info of the specified date not exists,
        # try again with the yesterday's exchange info
        # data_date = datetime.now() - pd.Timedelta(days=1)
        data_date = datetime.now()
        df_exchange_info = get_symbols(bd, data_date)

    tomorrow = date + timedelta(days=1)
    df_exchange_info = df_exchange_info.query('deliveryDate > @tomorrow and onboardDate <= @date')
    symbols = df_exchange_info['symbol']

    for symbol in symbols:
        try:
            df_instrument_funding_rate = bd.get_fapi_funding_rate(symbol, date)
            df_new_funding_rate=pd.DataFrame({
                'symbol':[symbol],
                'date':[date],
                'fundingRateDaily':[df_instrument_funding_rate['fundingRate'].apply(Decimal).sum()]
            })
            dfs.append(df_new_funding_rate)
        except FileNotFoundError:
            pass

    df_funding_rate = pd.concat(dfs, ignore_index=True)
    if df_funding_rate.empty:
        print('no data', file=sys.stderr)
        sys.exit(1)

    columns = ['symbol', 'date', 'fundingRateDaily']
    df_funding_rate = df_funding_rate[columns]

    print(df_funding_rate)

    output_dir = os.path.dirname(output_filepath)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df_funding_rate.to_csv(output_filepath, index=False)

    time_end = time.time()

    print(f'{len(df_funding_rate)} rows written to {output_filepath}, {time_end-time_start:.3f}s')


if __name__ == '__main__':
    main()

"""
python -m rafdb.funding_rate --date=20220101
"""