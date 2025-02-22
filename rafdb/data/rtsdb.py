from io import BytesIO
from urllib.parse import urljoin
import os
import time
import pandas as pd
import requests


class DataNotFound(FileNotFoundError):
    pass


class RTSDataService:
    """
    Reality Time-Series Data Service.
    """

    def __init__(self, root_dir, cache_dir=None, **kwargs):
        self.root_dir = root_dir
        self.cache_dir = cache_dir
        self._default_file_ext = kwargs.pop('file_ext', 'csv')
        self._session = requests.Session()

    def _read_file(self, path) -> "BytesIO":
        if self.cache_dir and os.path.exists(os.path.join(self.cache_dir, path)):
            return open(os.path.join(self.cache_dir, path), 'r', encoding='utf8')

        if self.root_dir.startswith('http'):
            url = urljoin(self.root_dir, path)
            res = self._session.get(url, timeout=30)
            try:
                res.raise_for_status()
                time.sleep(0.1)
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
            filepath = os.path.join(self.root_dir, path)
            try:
                return open(filepath, 'r', encoding='utf8')
            except FileNotFoundError:
                raise DataNotFound(filepath)

    def build_path(self, dataset_name, end_date, symbol=None, file_ext:str=None):
        if not file_ext:
            file_ext = self._default_file_ext

        file_ext = file_ext.lstrip('.')

        if symbol:
            return f'{dataset_name}/{end_date:%Y}/{end_date:%Y%m%d}/{symbol}.{file_ext}'
        else:
            return f'{dataset_name}/{end_date:%Y}/{end_date:%Y%m%d}/{dataset_name}.{file_ext}'

    def read_csv(self, filepath, **kwargs) -> pd.DataFrame:
        return pd.read_csv(self._read_file(filepath), **kwargs)