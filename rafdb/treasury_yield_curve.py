"""
treasury yield curse data
https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve&field_tdr_date_value_month=202308
"""

import argparse
from datetime import datetime, timedelta
import os
import sys
import pandas as pd
from .data import treasury
from .contractinfo import get_contracts


def get_data_service() -> "treasury.TreasuryData":
    """
    Get the data service object for CoinMarketCap data.
    """
    return treasury.TreasuryData()


def main():
    parser = argparse.ArgumentParser(description='Treasury Yield Curve')
    parser.add_argument('--date', '-d', default='20210101', help='date')
    parser.add_argument('--output', '-o', help='output filepath')
    parser.add_argument('--force', '-f', action='store_true', default=False, help='force write data file')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='verbose output')
    args = parser.parse_args()

    verbose = args.verbose
    date = datetime.strptime(args.date, '%Y%m%d')
    output_filepath = args.output or f'output/treasury_yield_curve/{date:%Y}/{date:%Y%m%d}/treasury_yield_curve.csv'

    data_service = get_data_service()

    for i in range(7):
        fetch_date = date - timedelta(days=i)
        try:
            df_all_yield_curve = data_service.get_yield_curve_exact(fetch_date)
            break
        except treasury.DataNotFound as e:
            print(e)

    df_all_yield_curve = df_all_yield_curve.query('NEW_DATE <= @date').iloc[-1:]

    if len(df_all_yield_curve) == 0:
        raise Exception("No data.")

    df_all_yield_curve['date'] = date

    df_all_yield_curve = df_all_yield_curve[['date', 'NEW_DATE', 'BC_1MONTH', 'BC_2MONTH', 'BC_3MONTH', 'BC_6MONTH', 'BC_1YEAR', 'BC_2YEAR', 'BC_3YEAR', 'BC_5YEAR', 'BC_7YEAR', 'BC_10YEAR', 'BC_20YEAR', 'BC_30YEAR', 'BC_30YEARDISPLAY']]

    if verbose:
        print(df_all_yield_curve)

    if os.path.exists(output_filepath) and not args.force:
        print(f'File {output_filepath} already exists. Use -f to overwrite.')
        sys.exit(1)

    output_dir = os.path.dirname(output_filepath)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df_all_yield_curve.to_csv(output_filepath, index=False)
    print(f'{len(df_all_yield_curve)} rows written to {output_filepath}')


if __name__ == '__main__':
    main()

"""
python -m rafdb.treasury_yield_curve --date=20220101
"""
