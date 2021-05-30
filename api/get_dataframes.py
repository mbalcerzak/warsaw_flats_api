# Query SQLite database and get pandas DataFrames
import pandas as pd
from utils import get_month


def get_area_categories():
    query = (
        "   CASE "
        "     WHEN flat_area <= 20 THEN '20_or_less' "
        "     WHEN flat_area <= 30 THEN '20_30' "
        "     WHEN flat_area <= 40 THEN '30_40' "
        "     WHEN flat_area <= 50 THEN '40_50' "
        "     WHEN flat_area <= 60 THEN '50_60' "
        "     WHEN flat_area <= 70 THEN '60_70' "
        "     WHEN flat_area <= 80 THEN '70_80' "
        "   ELSE '80_or_more' "
        "END as area_category "
    )

    return query


def get_flats_db():
    query = (
        "SELECT "
            "ad_id, "
            "location, "
            "flat_area, "
            "date_scraped  "
        "FROM flats "
        "WHERE flat_area > 0"
    )

    return query


def calc_avg_price():
    return "ROUND((prices.price/flat_area),2)"


def load_df(conn=None) -> pd.DataFrame:
    """
    Queries SQlite database, merges two tables and retrieves a DataFrame
    """
    df = pd.read_sql_query(
        "SELECT * , "
        f" {get_area_categories()} "
        "FROM flats WHERE flat_area > 0",
        conn)

    assert len(df) > 0, "No data loaded from the database"

    df = df.sort_values(by=['date_scraped'])

    return df


def load_df_avg_prices(conn=None) -> pd.DataFrame:
    """
    Queries SQlite database, merges two tables and retrieves a DataFrame
    """

    df = pd.read_sql_query(
        "SELECT "
        "   location, "
        "   SUBSTR(date_scraped, 6,2) as month_num, "
        f"  {calc_avg_price()} as avg_price_per_m,"
        "   count(*) as num_flats "
        "FROM prices "
        f"INNER JOIN ({get_flats_db()}) as flats "
        "ON prices.flat_id = flats.ad_id "
        "GROUP BY location, month_num "
        "HAVING price > 0 ",
        conn)

    assert len(df) > 0, "No data loaded from the database"

    df["month"] = df['month_num'].apply(get_month)
    df = df.drop(columns=['month_num'])
    df['avg_price_per_m'] = df['avg_price_per_m'].apply(int)

    return df


def load_area_cat_df(conn=None) -> pd.DataFrame:
    """
    Queries SQlite database, merges two tables and retrieves a DataFrame
    """
    df = pd.read_sql_query(
        "SELECT "
        "   location, "
        "   SUBSTR(date_scraped, 6,2) as month_num, "
        f"  {calc_avg_price()} as avg_price_per_m,"
        "   count(*) as num_flats,"
        f" {get_area_categories()} "
        "FROM prices "
        f"INNER JOIN ({get_flats_db()}) as flats "
        "ON prices.flat_id = flats.ad_id "
        "GROUP BY location, month_num, area_category "
        "HAVING price > 0 ",
        conn)

    assert len(df) > 0, "No data loaded from the database"

    df["month"] = df['month_num'].apply(get_month)
    df = df.drop(columns=['month_num'])
    df['avg_price_per_m'] = df['avg_price_per_m'].apply(int)

    return df
