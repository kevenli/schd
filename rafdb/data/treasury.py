from datetime import datetime, timedelta
from io import BytesIO
import logging
import os.path
from urllib.parse import urljoin
import time
from bs4 import BeautifulSoup
import pandas as pd
import requests
from .util import ensure_date, ensure_datetime, iter_dates

logger = logging.getLogger(__name__)


class DataNotFound(FileNotFoundError):
    pass


class TreasuryData:
    def __init__(self, root_dir=None):
        if root_dir is None:
            root_dir = os.environ.get('GOV_TREASURY_ROOT')
        
        if not root_dir:
            root_dir = 'https://gov-treasury.sgp1.digitaloceanspaces.com/'
            
        self.root_dir = root_dir

    def _read_file(self, path) -> "BytesIO":
        if self.root_dir.startswith('http'):
            url = urljoin(self.root_dir, path)
            res = requests.get(url)
            try:
                res.raise_for_status()
                time.sleep(0.1)
                return BytesIO(res.content)
            except requests.exceptions.HTTPError as ex:
                if ex.response.status_code in (403, 404):
                    raise DataNotFound(path) from ex
                raise
        else:
            filepath = os.path.join(self.root_dir, path)
            if not os.path.exists(filepath):
                raise DataNotFound(filepath)
            
            return open(filepath, 'r')

    def _build_filepath(self, datatype, relative_path, *relative_paths):
        return os.path.join(self.root_dir, datatype, relative_path, *relative_paths)
    
    def _build_yield_curve_path(self, date):
        filepath = os.path.join(self.root_dir, 'daily_treasury_yield_curve', f'{date:%Y}', f'{date:%Y%m%d}', 'daily_treasury_yield_curve.xml')
        if os.path.exists(filepath):
            return filepath
        
        # old filepath style.
        logger.warn('filepath not found, using old stype filepath. %s', filepath)
        return self._build_filepath('daily_treasury_yield_curve', f'{date:%Y}', f'{date:%Y%m%d}.xml')

    def get_yield_curve(self, date, fill=False):
        """
          parameters:
            :fill : whether fill na data with previous data. default to FALSE.
        """
        date = ensure_date(date)
        utc_now = datetime.utcnow() - timedelta(days=1)
        if date.year == utc_now.year:
            file_path = self._build_yield_curve_path(utc_now)
        else:
            file_path = self._build_yield_curve_path(date)

        logger.debug(file_path)

        with open(file_path, 'r') as f:
            file_content = f.read()

        bs_data = BeautifulSoup(file_content, 'xml')
        entries = bs_data.find_all('entry')
        items = []
        for entry in entries[::-1]:
            entry_date = datetime.strptime(entry.NEW_DATE.text, '%Y-%m-%dT%H:%M:%S')
            if entry_date > date:
                continue

            if fill and entry_date < date:
                # have found the last day
                fill = False 
                date = entry_date

            if not fill and entry_date < date:
                continue

            item = {}
            item['NEW_DATE'] = entry_date
            item['BC_1MONTH'] = float(entry.BC_1MONTH.text)
            item['BC_2MONTH'] = float(entry.BC_2MONTH.text)
            item['BC_3MONTH'] = float(entry.BC_3MONTH.text)
            item['BC_6MONTH'] = float(entry.BC_6MONTH.text)
            item['BC_1YEAR'] = float(entry.BC_1YEAR.text)
            item['BC_2YEAR'] = float(entry.BC_2YEAR.text)
            item['BC_3YEAR'] = float(entry.BC_3YEAR.text)
            item['BC_5YEAR'] = float(entry.BC_5YEAR.text)
            item['BC_7YEAR'] = float(entry.BC_7YEAR.text)
            item['BC_10YEAR'] = float(entry.BC_10YEAR.text)
            item['BC_20YEAR'] = float(entry.BC_20YEAR.text)
            item['BC_30YEAR'] = float(entry.BC_30YEAR.text)
            item['BC_30YEARDISPLAY'] = float(entry.BC_30YEARDISPLAY.text)
            items.append(item)

        return pd.DataFrame(items)

    def get_yield_curve_by_year(self, year:int) -> 'pd.DataFrame':
        utc_now = datetime.utcnow() - timedelta(days=1)
        if year == utc_now.year:
            file_path = self._build_yield_curve_path(utc_now)
        else:
            file_path = self._build_yield_curve_path(datetime(year, 12, 31))
        with open(file_path, 'r') as f:
            file_content = f.read()

        bs_data = BeautifulSoup(file_content, 'xml')
        entries = bs_data.find_all('entry')
        items = []
        for entry in entries:
            entry_date = datetime.strptime(entry.NEW_DATE.text, '%Y-%m-%dT%H:%M:%S')
            item = {}
            item['NEW_DATE'] = entry_date
            item['BC_1MONTH'] = float(entry.BC_1MONTH.text)
            item['BC_2MONTH'] = float(entry.BC_2MONTH.text)
            item['BC_3MONTH'] = float(entry.BC_3MONTH.text)
            item['BC_6MONTH'] = float(entry.BC_6MONTH.text)
            item['BC_1YEAR'] = float(entry.BC_1YEAR.text)
            item['BC_2YEAR'] = float(entry.BC_2YEAR.text)
            item['BC_3YEAR'] = float(entry.BC_3YEAR.text)
            item['BC_5YEAR'] = float(entry.BC_5YEAR.text)
            item['BC_7YEAR'] = float(entry.BC_7YEAR.text)
            item['BC_10YEAR'] = float(entry.BC_10YEAR.text)
            item['BC_20YEAR'] = float(entry.BC_20YEAR.text)
            item['BC_30YEAR'] = float(entry.BC_30YEAR.text)
            item['BC_30YEARDISPLAY'] = float(entry.BC_30YEARDISPLAY.text)
            items.append(item)

        return pd.DataFrame(items)

    def _build_yield_curve_exact_path(self, date):
        filepath = os.path.join(self.root_dir, 'daily_treasury_yield_curve_exact', f'{date:%Y}', f'{date:%Y%m%d}', 'daily_treasury_yield_curve_exact.xml')
        return filepath

    def _read_yield_curve_exact_file(self, file_path, datadate):
        with open(file_path, 'r' ,encoding='utf8') as f:
            file_content = f.read()

        bs_data = BeautifulSoup(file_content, 'xml')
        entries = bs_data.find_all('entry')
        items = []
        for entry in entries[::-1]:
            entry_date = datetime.strptime(entry.NEW_DATE.text, '%Y-%m-%dT%H:%M:%S')
            if entry_date != datadate:
                continue

            item = {}
            item['NEW_DATE'] = entry_date
            item['BC_1MONTH'] = float(entry.BC_1MONTH.text)
            item['BC_2MONTH'] = float(entry.BC_2MONTH.text)
            item['BC_3MONTH'] = float(entry.BC_3MONTH.text)
            item['BC_6MONTH'] = float(entry.BC_6MONTH.text)
            item['BC_1YEAR'] = float(entry.BC_1YEAR.text)
            item['BC_2YEAR'] = float(entry.BC_2YEAR.text)
            item['BC_3YEAR'] = float(entry.BC_3YEAR.text)
            item['BC_5YEAR'] = float(entry.BC_5YEAR.text)
            item['BC_7YEAR'] = float(entry.BC_7YEAR.text)
            item['BC_10YEAR'] = float(entry.BC_10YEAR.text)
            item['BC_20YEAR'] = float(entry.BC_20YEAR.text)
            item['BC_30YEAR'] = float(entry.BC_30YEAR.text)
            item['BC_30YEARDISPLAY'] = float(entry.BC_30YEARDISPLAY.text)
            items.append(item)

        return pd.DataFrame(items, columns=['NEW_DATE', 'BC_1MONTH', 'BC_2MONTH', 'BC_3MONTH', 'BC_6MONTH', 'BC_1YEAR', 'BC_2YEAR', 'BC_3YEAR', 'BC_5YEAR', 'BC_7YEAR', 'BC_10YEAR', 'BC_20YEAR', 'BC_30YEAR', 'BC_30YEARDISPLAY'])

    def search_latest_yield_curve_exact_file(self, date):
        """
        search the latest yield curve file which exactly match the date.
        """
        loop_date = ensure_date(date)
        while True:
            file_path = self._build_yield_curve_exact_path(loop_date)
            if os.path.exists(file_path):
                return self._read_yield_curve_exact_file(file_path, loop_date)
            
            # from the target date, descending order search.
            loop_date = loop_date - timedelta(days=1)

            if loop_date < date - timedelta(days=10):
                # if over 10 days an available data file is not found.
                # raise an error.
                raise RuntimeError('Cannot find the latest yield curve file. %s' % loop_date)

    def get_yield_curve_exact(self, datadate):
        datadate = ensure_datetime(datadate)
        filepath = f'daily_treasury_yield_curve_exact/{datadate:%Y}/{datadate:%Y%m%d}/daily_treasury_yield_curve_exact.xml'
        file_content = self._read_file(filepath)

        bs_data = BeautifulSoup(file_content, 'xml')
        entries = bs_data.find_all('entry')
        items = []
        for entry in entries[::-1]:
            entry_date = datetime.strptime(entry.NEW_DATE.text, '%Y-%m-%dT%H:%M:%S')
            if entry_date.date() < datadate.date():
                # data are descending sorted, if current iter entry_date is already prior
                # to the specified date, there would be no more data to be found.
                raise DataNotFound('Cannot find the data. %s' % datadate)
            
            if entry_date.date() != datadate.date():
                continue

            item = {}
            item['NEW_DATE'] = entry_date
            item['BC_1MONTH'] = float(entry.BC_1MONTH.text)
            item['BC_2MONTH'] = float(entry.BC_2MONTH.text)
            item['BC_3MONTH'] = float(entry.BC_3MONTH.text)
            item['BC_6MONTH'] = float(entry.BC_6MONTH.text)
            item['BC_1YEAR'] = float(entry.BC_1YEAR.text)
            item['BC_2YEAR'] = float(entry.BC_2YEAR.text)
            item['BC_3YEAR'] = float(entry.BC_3YEAR.text)
            item['BC_5YEAR'] = float(entry.BC_5YEAR.text)
            item['BC_7YEAR'] = float(entry.BC_7YEAR.text)
            item['BC_10YEAR'] = float(entry.BC_10YEAR.text)
            item['BC_20YEAR'] = float(entry.BC_20YEAR.text)
            item['BC_30YEAR'] = float(entry.BC_30YEAR.text)
            item['BC_30YEARDISPLAY'] = float(entry.BC_30YEARDISPLAY.text)
            items.append(item)
            break

        return pd.DataFrame(items, columns=['NEW_DATE', 'BC_1MONTH', 'BC_2MONTH', 'BC_3MONTH', 'BC_6MONTH',
                                            'BC_1YEAR', 'BC_2YEAR', 'BC_3YEAR', 'BC_5YEAR', 'BC_7YEAR', 'BC_10YEAR',
                                            'BC_20YEAR', 'BC_30YEAR', 'BC_30YEARDISPLAY'])

    def get_yield_curve_exact_ts(self, start_date, end_date):
        start_date = ensure_date(start_date)
        end_date = ensure_date(end_date)
        dfs = []
        for d in iter_dates(start_date, end_date):
            try:
                dfs.append(self.get_yield_curve_exact(d))
            except DataNotFound:
                pass
        
        return pd.concat(dfs)
