from collections import Counter
import json
import yaml
import sqlite3
import pandas as pd
import psutil

from get_dataframes import load_df, load_df_avg_prices, load_area_cat_df
from utils import get_moving_avg, today_str, get_month, remove_waw, get_weekday, dict_counter


def process_df(df: pd.DataFrame = None) -> pd.DataFrame:
    if 'date_scraped' in list(df):
        df['month'] = df['date_scraped'].apply(get_month)
        df['weekday'] = df['date_scraped'].apply(get_weekday)

    df['location'] = df['location'].apply(remove_waw)

    if 'avg_price_per_m' in list(df):
        df['avg_price_per_m'] = df['avg_price_per_m'].apply(int)

    return df


def get_flats_stats(conn=None) -> dict:
    """ Create statistics about the scraped data """

    df_flats = load_df(conn)
    df_flats = process_df(df_flats)
    today = today_str()
    df_flats = df_flats.loc[df_flats['date_scraped'] != today]

    posted_per_day = dict_counter(df_flats, 'date_posted')
    scraped_per_day = dict_counter(df_flats, 'date_scraped')
    scraped_per_day_m_avg = get_moving_avg(scraped_per_day, 7)
    scraped_per_month = dict_counter(df_flats, 'month')

    flats_per_location = dict_counter(df_flats, 'location')
    flats_per_area_cat = dict_counter(df_flats, 'area_category')

    price_m_location = load_df_avg_prices(conn)
    price_m_loc_area_cat = load_area_cat_df(conn)

    price_m_location = process_df(price_m_location).to_dict('record')
    price_m_loc_area_cat = process_df(price_m_loc_area_cat).to_dict('record')

    return {
        "flats_per_location": flats_per_location,
        "flats_per_area_cat": flats_per_area_cat,
        "scraped_per_day": scraped_per_day,
        "scraped_per_day_m_avg": scraped_per_day_m_avg,
        "scraped_per_month": scraped_per_month,
        "posted_per_day": posted_per_day,
        "price_m_location": price_m_location,
        "price_m_loc_area_cat": price_m_loc_area_cat
    }


if __name__ == "__main__":
    with open(r'config.yaml') as f:
        paths = yaml.safe_load(f)

    data_path = paths['data_path']

    try:
        connection = sqlite3.connect(data_path, check_same_thread=False)
        df = get_flats_stats(connection)

        with open('json_dir/flats.json', 'w') as f:
            json.dump(df, f, ensure_ascii=False)

    except sqlite3.Error as e:
        raise Exception
