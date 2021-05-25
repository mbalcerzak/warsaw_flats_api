import pandas as pd
from collections import Counter, OrderedDict
import sqlite3
import json
import yaml


def load_df(path=None) -> pd.DataFrame:
    """
    Queries SQlite database, merges two tables and retrieves a DataFrame
    """
    try:
        conn = sqlite3.connect(path)
    except sqlite3.Error as e:
        raise Exception

    df = pd.read_sql_query(
        "SELECT * , "
        "   CASE "
        "     WHEN flat_area <= 20 THEN 'less20' "
        "     WHEN flat_area <= 30 THEN '20_30' "
        "     WHEN flat_area <= 40 THEN '30_40' "
        "     WHEN flat_area <= 50 THEN '40_50' "
        "     WHEN flat_area <= 60 THEN '50_60' "
        "     WHEN flat_area <= 70 THEN '60_70' "
        "     WHEN flat_area <= 80 THEN '70_80' "
        "   ELSE 'more80' "
        "END as area_category "
        "FROM flats WHERE flat_area > 0",
        conn)

    assert len(df) > 0, "No data loaded from the database"

    return df


def load_df_avg_prices(path=None) -> pd.DataFrame:
    """
    Queries SQlite database, merges two tables and retrieves a DataFrame
    """
    try:
        conn = sqlite3.connect(path)
    except sqlite3.Error as e:
        raise Exception

    df = pd.read_sql_query(
        "SELECT "
        "   location, "
        "   SUBSTR(date_scraped, 6,2) as month, "
        "   avg(CAST(ROUND(prices.price/flat_area,2) as decimal(10,1))) as avg_price_per_m,"
        "   count(*) as num_flats "
        "FROM prices "
        "INNER JOIN (SELECT "
                        "ad_id, "
                        "location, "
                        "flat_area, "
                        "date_scraped  "
                     "FROM flats "
                     "WHERE flat_area > 0) as flats "
        "ON prices.flat_id = flats.ad_id "
        "GROUP BY location, month "
        "HAVING price > 0 ",
        conn)

    assert len(df) > 0, "No data loaded from the database"

    return df


def load_area_cat_df(path=None) -> pd.DataFrame:
    """
    Queries SQlite database, merges two tables and retrieves a DataFrame
    """
    try:
        conn = sqlite3.connect(path)
    except sqlite3.Error as e:
        raise Exception

    df = pd.read_sql_query(
        "SELECT "
        "   location, "
        "   SUBSTR(date_scraped, 6,2) as month, "
        "   avg(CAST(ROUND(prices.price/flat_area,2) as decimal(10,1))) as avg_price_per_m,"
        "   count(*) as num_flats,"
        "   CASE "
        "     WHEN flat_area <= 20 THEN 'less20' "
        "     WHEN flat_area <= 30 THEN '20_30' "
        "     WHEN flat_area <= 40 THEN '30_40' "
        "     WHEN flat_area <= 50 THEN '40_50' "
        "     WHEN flat_area <= 60 THEN '50_60' "
        "     WHEN flat_area <= 70 THEN '60_70' "
        "     WHEN flat_area <= 80 THEN '70_80' "
        "   ELSE 'more80' "
        "END as area_category "
        "FROM prices "
        "INNER JOIN (SELECT "
                        "ad_id, "
                        "location, "
                        "flat_area, "
                        "date_scraped  "
                     "FROM flats "
                     "WHERE flat_area > 0) as flats "
        "ON prices.flat_id = flats.ad_id "
        "GROUP BY location, month, area_category "
        "HAVING price > 0 ",
        conn)

    assert len(df) > 0, "No data loaded from the database"

    return df


def get_flats_stats(path=None) -> dict:
    """
    Create statistics about the scraped data
    """
    df_flats = load_df(path)

    date_first = min(df_flats['date_scraped'])
    date_last = max(df_flats['date_scraped'])

    def dict_counter(col):
        return OrderedDict(Counter(df_flats[col]))

    flats_per_location = dict_counter('location')
    scraped_per_day = dict_counter('date_scraped')
    posted_per_day = dict_counter('date_posted')
    flats_per_area_cat = dict_counter('area_category')

    price_m_location = load_df_avg_prices(path).to_dict('record')
    price_m_loc_area_cat = load_area_cat_df(path).to_dict('record')

    return {
        "date_first": date_first,
        "date_last": date_last,
        "flats_per_location": flats_per_location,
        "flats_per_area_cat": flats_per_area_cat,
        "scraped_per_day": scraped_per_day,
        "posted_per_day": posted_per_day,
        "price_m_location": price_m_location,
        "price_m_loc_area_cat": price_m_loc_area_cat
    }


if __name__ == "__main__":
    with open(r'config.yaml') as f:
        paths = yaml.safe_load(f)

    df = get_flats_stats(paths['data_path'])

    with open('json_dir/flats.json', 'w') as f:
        json.dump(df, f, ensure_ascii=False)
