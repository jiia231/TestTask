from sys import stdout

import pandas as pd
import requests

'''
Задание.
Есть две биржи: binance и okex. 
Нужно сделать простой словарь: необходимо получить список 
доступных инструментов с этих бирж и составить их в одну таблицу,
 в которой есть три колонки: унифицированное название инструмента, 
 биржевое название binance, биржевое название okex. 
Отсортировать по унифицированному названию. 
Формат csv: unify name, binance name, okex name. 
Результат должен быть выведен в терминал.
'''

BINANCE_URL: str = 'https://api.binance.com/api/v3/exchangeInfo'
OKEX_URL: str = 'https://www.okex.com/api/spot/v3/instruments'
UNIFY_COLUMN_NAME: str = 'unify name'
BINANCE_COLUMN_NAME: str = 'binance name'
OKEX_COLUMN_NAME: str = 'okex name'


def get_binance_df() -> pd.DataFrame:
    response: dict = requests.get(BINANCE_URL).json()
    df = pd.DataFrame.from_dict(response['symbols'])[
        ['symbol', 'baseAsset', 'quoteAsset']
    ]
    df.rename({'symbol': BINANCE_COLUMN_NAME}, axis=1, inplace=True)
    df[UNIFY_COLUMN_NAME] = df[['baseAsset', 'quoteAsset']].sum(axis=1)
    df.drop(['baseAsset', 'quoteAsset'], axis=1, inplace=True)
    return df


def get_okex_df() -> pd.DataFrame:
    response: list = requests.get(OKEX_URL).json()
    df = pd.DataFrame.from_dict(response)[
        ['instrument_id', 'base_currency', 'quote_currency']
    ]
    df.rename({'instrument_id': OKEX_COLUMN_NAME}, axis=1, inplace=True)
    df[UNIFY_COLUMN_NAME] = df[['base_currency', 'quote_currency']].sum(axis=1)
    df.drop(['base_currency', 'quote_currency'], axis=1, inplace=True)
    return df


def merge_instruments_df(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    result_df = df1.merge(df2, on=UNIFY_COLUMN_NAME, how='outer')[
        [UNIFY_COLUMN_NAME, BINANCE_COLUMN_NAME, OKEX_COLUMN_NAME]
    ]
    result_df.sort_values(UNIFY_COLUMN_NAME, inplace=True, ignore_index=True)
    return result_df


if __name__ == '__main__':
    binance_df = get_binance_df()
    okex_df = get_okex_df()
    exchanges_instruments = merge_instruments_df(binance_df, okex_df)
    exchanges_instruments.to_csv(stdout, index=False)
