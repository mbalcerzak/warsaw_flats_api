from collections import Counter
import json
import yaml
import sqlite3

from get_dataframes import load_df, load_df_avg_prices, load_area_cat_df
from utils import get_moving_avg, get_date_weekdays


def get_flats_stats(conn=None) -> dict:
    """
    Create statistics about the scraped data
    """
    df_flats = load_df(conn)

    print(df_flats.head())

    date_first = min(df_flats['date_scraped'])
    date_last = max(df_flats['date_scraped'])

    def dict_counter(dataframe=None, col=None):
        dataframe = dataframe.sort_values(by=[col])
        return dict(Counter(dataframe[col]))

    flats_per_location = dict_counter(df_flats, 'location')

    scraped_per_day = dict_counter(df_flats, 'date_scraped')
    scraped_per_day_m_avg = get_moving_avg(scraped_per_day, 7)
    scraped_per_month = dict_counter(df_flats, 'month')

    date_weekdays = get_date_weekdays(df_flats)

    posted_per_day = dict_counter(df_flats, 'date_posted')
    flats_per_area_cat = dict_counter(df_flats, 'area_category')

    price_m_location = load_df_avg_prices(conn).to_dict('record')
    price_m_loc_area_cat = load_area_cat_df(conn).to_dict('record')

    return {
        "date_first": date_first,
        "date_last": date_last,
        "flats_per_location": flats_per_location,
        "flats_per_area_cat": flats_per_area_cat,
        "scraped_per_day": scraped_per_day,
        "scraped_per_day_m_avg": scraped_per_day_m_avg,
        "date_weekdays": date_weekdays,
        "scraped_per_month": scraped_per_month,
        "posted_per_day": posted_per_day,
        "price_m_location": price_m_location,
        "price_m_loc_area_cat": price_m_loc_area_cat
    }


if __name__ == "__main__":
    with open(r'config.yaml') as f:
        paths = yaml.safe_load(f)

    data_path = paths['data_path']
    print(data_path)

    try:
        connection = sqlite3.connect(data_path)
        df = get_flats_stats(connection)

        with open('json_dir/flats.json', 'w') as f:
            json.dump(df, f, ensure_ascii=False)

    except sqlite3.Error as e:
        raise Exception
