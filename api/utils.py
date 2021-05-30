from collections import Counter

import pandas as pd
from datetime import datetime


def get_month(date: str) -> str:
    """Selects a month number from a date (String) and outputs name of the month"""
    months = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December"
    }
    return months[int(date)]


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


def get_date_weekdays(df):
    weekdays = list(df['weekday'].unique())
    weekdays_json = {}

    for weekday in weekdays:
        day_df = df.loc[df['weekday'] == weekday]
        weekdays_json[weekday] = list(day_df['date_scraped'].unique())

    return weekdays_json
