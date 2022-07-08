import pandas as pd
import os
from .schema import Schema

from bscsi_data.optm_lz.load import load_for_tickers


class OptionMetricsData:
    """Historical Options Data container class."""
    def __init__(self, tickers, schema=None, **params):
        if schema is None:
            self.schema = OptionMetricsData.default_schema()

        self._data = load_for_tickers(tickers).reset_index().rename(columns={'secid': 'underlying',
            'forward_price': 'underlying_last',
            'optionid': 'contract',
            'cp_flag': 'type',
            'exdate': 'expiration',
            'strike_price': 'strike',
            'best_bid': 'bid',
            'best_offer': 'ask',
            'open_interest': 'open_interest',
            'impl_volatility': 'impliedvol'})
        self._data['type'] = self._data['type'].replace('C', 'call').replace('P', 'put')
        self._data['contract'] = self._data['contract'].astype(str)
        # self._data['date']=self._data.date.dt.tz_localize('utc')
        # self._data['exdate']=self._data['exdate'].dt.tz_localize('utc')
        # print(self._data.head(5))

        columns = self._data.columns
        for _key, col in self.schema:
            assert col in columns, f"missing '{col}' in data"
        # assert all((col in columns for _key, col in self.schema))

        date_col = self.schema['date']
        expiration_col = self.schema['expiration']

        self._data['dte'] = (self._data[expiration_col] - self._data[date_col]).dt.days
        self.schema.update({'dte': 'dte'})

        self.start_date = self._data[date_col].min()
        self.end_date = self._data[date_col].max()

    def apply_filter(self, f):
        """Apply Filter `f` to the data. Returns a `pd.DataFrame` with the filtered rows."""
        return self._data.query(f.query)

    def iter_dates(self):
        """Returns `pd.DataFrameGroupBy` that groups contracts by date"""
        return self._data.groupby(self.schema['date'])

    def iter_months(self):
        """Returns `pd.DataFrameGroupBy` that groups contracts by month"""
        date_col = self.schema['date']
        iterator = self._data.groupby(pd.Grouper(
            key=date_col,
            freq="MS")).apply(lambda g: g[g[date_col] == g[date_col].min()]).reset_index(drop=True).groupby(date_col)
        return iterator

    def __getattr__(self, attr):
        """Pass method invocation to `self._data`"""

        method = getattr(self._data, attr)
        if hasattr(method, '__call__'):

            def df_method(*args, **kwargs):
                return method(*args, **kwargs)

            return df_method
        else:
            return method

    def __getitem__(self, item):
        if isinstance(item, pd.Series):
            return self._data[item]
        else:
            key = self.schema[item]
            return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value
        if key not in self.schema:
            self.schema.update({key: key})

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return self._data.__repr__()

    def default_schema():
        """Returns default schema for Historical Options Data"""
        schema = Schema.options()
        # schema.update({
        #     'underlying': 'secid',
        #     'underlying_last': 'forward_price',  #last price of underlying
        #     'date': 'date',
        #     'contract': 'optionid',
        #     'type': 'cp_flag',
        #     'expiration': 'exdate',
        #     'strike': 'strike_price',
        #     'bid': 'best_bid',
        #     'ask': 'best_offer',
        #     'volume': 'volume',
        #     'open_interest': 'open_interest',
        #     'last': 'forward_price',
        #     'impliedvol': 'impl_volatility',
        #     'vega': 'vega',
        #     'delta': 'delta',
        #     'gamma': 'gamma',
        #     'theta': 'theta',
        # })
        return schema
