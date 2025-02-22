"""
produce contractinfo data table.
"""
import argparse
from datetime import datetime
import os
import pandas as pd
from rafdb.data.binance import BinanceData, DataNotFound


def get_symbols(binance_data:"BinanceData", date:datetime) -> "pd.DataFrame":
    """
    get exchange_info of specified date
    """
    df_exchange_info = binance_data.get_fapi_exchange_info_symbols(date)
    # ignore stable coins
    # pylint: disable=unused-variable
    ignore_symbol_list = ['USDCUSDT']
    # store only perpetual contracts with USDT as quote asset
    filter_expr = 'contractType == "PERPETUAL" and quoteAsset == "USDT" and symbol not in @ignore_symbol_list'
    df_exchange_info = df_exchange_info.query(filter_expr).sort_values('symbol').copy()
    return df_exchange_info


def get_contracts(binance_data:"BinanceData", date) -> "pd.DataFrame":
    """
    get future contracts.
    contracts must satisfy the following roles:
      1. contractType is "PERPETUAL"
      2. quoteAsset is "USDT"
      3. baseAsset is not stable-coin, i.e. USDCUSDT
      4. underlyingType is "COIN"
      5. onboardDate is not later than @date

    for historical data already lost, use the latest exchange_info to rebuild.
    """
    try:
        df_exchange_info = binance_data.get_fapi_exchange_info_symbols(date)

    except DataNotFound:
        # if exchange_info of the specified date not exists,
        # try again with the latest exchange info
        # data_date = datetime.now() - pd.Timedelta(days=1)
        data_date = datetime.now()
        df_exchange_info = binance_data.get_fapi_exchange_info_symbols(data_date)

        # when using filled data, modify status according deliveryDate
        df_exchange_info['status'] = df_exchange_info['deliveryDate'].apply(lambda x: 'TRADING' if x > date else 'SETTLING')

    # ignore stable coins
    # pylint: disable=unused-variable
    ignore_symbol_list = ['USDCUSDT']
    # store only perpetual contracts with USDT as quote asset
    filter_expr = 'contractType == "PERPETUAL" and quoteAsset == "USDT" and underlyingType == "COIN" and symbol not in @ignore_symbol_list and onboardDate <= @date'
    return df_exchange_info.query(filter_expr).copy()


def main():
    parser = argparse.ArgumentParser(description='produce contractinfo data table')
    parser.add_argument('--date', required=True, type=str, help='date in YYYYMMDD format')
    # output parameter
    parser.add_argument('--output', '-o', type=str, help='output filepath')

    args = parser.parse_args()
    date = datetime.strptime(args.date, '%Y%m%d')
    output_filepath = args.output or f'output/contractinfo/{date:%Y}/{date:%Y%m%d}/contractinfo.csv'
    
    binance_data = BinanceData()

    df_exchange_info = get_contracts(binance_data, date)
    df_exchange_info['date'] = date
    df_exchange_info.sort_values('symbol', inplace=True)
    columns = ['symbol','date','pair','contractType','deliveryDate','onboardDate','status',
               'maintMarginPercent','requiredMarginPercent','baseAsset','quoteAsset','marginAsset',
               'pricePrecision','quantityPrecision','baseAssetPrecision','quotePrecision',
               'underlyingType','settlePlan','triggerProtect','liquidationFee','marketTakeBound','lotSize']
    
    df_exchange_info = df_exchange_info[columns]
    print(df_exchange_info)

    output_dir = os.path.dirname(output_filepath)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df_exchange_info.to_csv(output_filepath, index=False)

    print(f'{len(df_exchange_info)} rows written to {output_filepath}')


if __name__ == '__main__':
    main()

"""
python -m rafdb.contractinfo --date=20220101
"""