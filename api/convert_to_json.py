import json
import yaml
import sqlite3
import pandas as pd

from get_dataframes import (
    get_price_m_location,
    get_price_m_loc_area_cat,
    get_scraped_per_day,
    get_price_changes_per_day,
    get_flats_per_area_cat,
    get_flats_per_location,
    get_posted_per_day,
    get_scraped_per_month,
    get_dates
)
from utils import (
    get_moving_avg,
    today_str,
    get_month,
    remove_waw,
    get_weekday,
    get_month_from_date,
    dict_counter
)


def process_df(df: pd.DataFrame = None) -> pd.DataFrame:
    if 'location' in list(df):
        df['location'] = df['location'].apply(remove_waw)
    if 'date_scraped' in list(df):
        df['weekday'] = df['date_scraped'].apply(get_weekday)
        df['month'] = df['date_scraped'].apply(get_month_from_date)
    if 'avg_price_per_m' in list(df):
        df['avg_price_per_m'] = df['avg_price_per_m'].apply(int)
    if 'month_num' in list(df):
        df['month'] = df['month_num'].apply(get_month)

    return df


def get_flats_stats(conn=None) -> dict:
    """ Create statistics about the scraped data """
    today = today_str()

    print("Calculating posted_per_day... ")
    posted_per_day = get_posted_per_day(conn)
    posted_per_day = posted_per_day.loc[posted_per_day['date_posted'] != today]
    posted_per_day_m_avg = get_moving_avg(posted_per_day, 7)
    posted_per_day = dict_counter(posted_per_day, 'date_posted')

    print("Calculating scraped_per_month... ")
    scraped_per_month = get_scraped_per_month(conn)
    scraped_per_month = process_df(scraped_per_month)
    scraped_per_month = dict_counter(scraped_per_month, 'month')

    print("Calculating flats_per_location... ")
    flats_per_location = get_flats_per_location(conn)
    flats_per_location = process_df(flats_per_location)
    flats_per_location = dict_counter(flats_per_location, 'location')

    print("Calculating flats_per_area_cat... ")
    flats_per_area_cat = get_flats_per_area_cat(conn)
    flats_per_area_cat = dict_counter(flats_per_area_cat, 'area_category')

    print("Calculating scraped_per_day... ")
    scraped_per_day = get_scraped_per_day(conn)
    scraped_per_day_df = scraped_per_day.loc[scraped_per_day['date_scraped'] != today]
    scraped_per_day = dict_counter(scraped_per_day_df, 'date_scraped')

    print("Calculating Moving Average of scraped per day... ")
    scraped_per_day_m_avg = get_moving_avg(scraped_per_day_df, 7)

    print('Calculating price changes per day...')
    price_changes_per_day = get_price_changes_per_day(conn)
    price_changes_per_day = price_changes_per_day.loc[price_changes_per_day['date_scraped'] != today]
    changes_per_day = dict_counter(price_changes_per_day, 'date_scraped')

    print("Calculating Moving Average of price changes... ")
    changed_per_day_m_avg = get_moving_avg(price_changes_per_day, 7)

    print("Loading get_price_m_location ... ")
    price_m_location = get_price_m_location(conn)
    price_m_location = process_df(price_m_location).to_dict('records')

    print("Loading get_price_m_loc_area_cat ... ")
    price_m_loc_area_cat = get_price_m_loc_area_cat(conn)
    price_m_loc_area_cat = process_df(price_m_loc_area_cat).to_dict('records')

    print("Find dates (max, min)")
    dates = get_dates(conn)

    return {
        "dates": dates,

        "flats_per_location": flats_per_location,
        "flats_per_area_cat": flats_per_area_cat,

        "scraped_per_day": scraped_per_day,
        "scraped_per_day_m_avg": scraped_per_day_m_avg,

        "changes_per_day": changes_per_day,
        "changed_per_day_m_avg": changed_per_day_m_avg,

        "scraped_per_month": scraped_per_month,
        "posted_per_day": posted_per_day,
        "posted_per_day_m_avg": posted_per_day_m_avg,

        "price_m_location": price_m_location,
        "price_m_loc_area_cat": price_m_loc_area_cat
    }


if __name__ == "__main__":
    with open(r'config.yaml') as f:
        paths = yaml.safe_load(f)

    data_path = paths['data_path']
    print(data_path)

    try:
        connection = sqlite3.connect(data_path, check_same_thread=False)
        df = get_flats_stats(connection)

        with open('json_dir/flats.json', 'w') as f:
            json.dump(df, f, ensure_ascii=False)

    except sqlite3.Error as e:
        raise Exception

    connection.close()
