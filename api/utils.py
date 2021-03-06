from collections import Counter
import pandas as pd
from datetime import datetime


def get_month(date: str) -> str:
    # """Selects a month number from a date (String) and outputs name of the month"""
    # months = {
    #     1: "January",
    #     2: "February",
    #     3: "March",
    #     4: "April",
    #     5: "May",
    #     6: "June",
    #     7: "July",
    #     8: "August",
    #     9: "September",
    #     10: "October",
    #     11: "November",
    #     12: "December"
    # }
    # return f"{months[int(date[-2:])]}"
    str_date = datetime.strptime(date, '%Y-%m')
    return str_date.strftime("%B%Y")


def get_month_from_date(date: str = None) -> str:
    str_date = datetime.strptime(date, '%Y-%m-%d')
    return str_date.strftime("%B%Y")


def remove_waw(loc: str = None) -> str:
    return loc.replace(", Warszawa", "")


def get_weekday(date: str = None) -> str:
    str_date = datetime.strptime(date, '%Y-%m-%d')
    return str_date.strftime("%A")


def get_moving_avg(scraped_per_day: dict, n: int = None) -> dict:
    try:
        df = scraped_per_day.sort_values(by=['date_scraped'])
        col = 'date_scraped'
    except KeyError:
        df = scraped_per_day.sort_values(by=['date_posted'])
        col = 'date_posted'

    df['num_flats'] = df['num_flats'].rolling(n).mean()
    df = df.dropna()

    json_dict = {}
    for _, row in df.iterrows():
        json_dict[row[col]] = int(row['num_flats'])

    return json_dict


def today_str():
    return datetime.today().strftime('%Y-%m-%d')


def dict_counter(df=None, col1=None):
    df = df.sort_values(by=[col1])
    json_dict = {}
    for _, row in df.iterrows():
        json_dict[row[col1]] = int(row['num_flats'])

    return json_dict
