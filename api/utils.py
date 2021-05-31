from collections import Counter

import pandas as pd
from datetime import datetime


def get_month(date: str) -> str:
    str_date = datetime.strptime(date, '%Y-%m-%d')
    return str_date.strftime("%B")


def remove_waw(loc: str = None) -> str:
    return loc.replace(", Warszawa", "")


def get_weekday(date: str = None) -> str:
    str_date = datetime.strptime(date, '%Y-%m-%d')
    return str_date.strftime("%A")


def get_moving_avg(scraped_per_day: dict, n: int = None) -> dict:
    df = pd.DataFrame(scraped_per_day.items(), columns=['Date', 'Value'])
    df = df.sort_values(by=['Date'])
    df['Value'] = df['Value'].rolling(n).mean()
    df = df.dropna()

    json_dict = {}
    for _, row in df.iterrows():
        json_dict[row['Date']] = int(row['Value'])

    return json_dict


def today_str():
    return datetime.today().strftime('%Y-%m-%d')


def dict_counter(dataframe=None, col=None):
    dataframe = dataframe.sort_values(by=[col])
    return dict(Counter(dataframe[col]))

