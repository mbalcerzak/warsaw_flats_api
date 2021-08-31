# Query SQLite database and get pandas DataFrames
from datetime import datetime
import pandas as pd

max_area = 200

def get_area_categories():
    return (
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


def get_flats_db():
    return (
        "SELECT ad_id, location, flat_area, date_scraped "
        "FROM flats "
        f"WHERE flat_area > 0 and flat_area < {max_area} "
        )


def calc_avg_price():
    return "ROUND((SUM(prices.price)/SUM(flat_area)),2)"


def get_scraped_per_day(conn=None) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT date_scraped, count(*) as num_flats "
        "FROM  flats "
        "GROUP BY date_scraped ",
        conn)

    return df


def get_price_changes_per_day(conn=None) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT date as date_scraped, count(*) as num_flats FROM prices "
        "WHERE flat_id in (SELECT flat_id "
                            "FROM prices "
                            "GROUP BY flat_id "
                            "HAVING count(flat_id) > 1) "
        "GROUP BY date",
        conn)

    return df


def get_scraped_per_month(conn=None) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT "
        "   SUBSTR(date_scraped, 6,2) as month_num, "
        "   count(*) as num_flats "
        "FROM  flats "
        "GROUP BY month_num ",
        conn)

    return df


def get_posted_per_day(conn=None) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT date_posted, count(*) as num_flats "
        "FROM  flats "
        "GROUP BY date_posted ",
        conn)

    return df


def get_flats_per_location(conn=None) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT "
        "   location ,"
        "   count(*) as num_flats "
        "FROM flats "
        "GROUP BY location "
        "ORDER BY location ",
        conn)

    return df


def get_flats_per_area_cat(conn=None) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT "
        f" {get_area_categories()} ,"
        "   count(*) as num_flats "
        "FROM flats "
        "GROUP BY area_category "
        "ORDER BY area_category ",
        conn)

    return df


def get_price_m_location(conn=None) -> pd.DataFrame:
    """
    Queries SQlite database, merges two tables and retrieves a DataFrame
    """
    df = pd.read_sql_query(
        "SELECT "
        "   location, "
        "   SUBSTR(date_scraped, 6,2) as month_num, "
        f"  {calc_avg_price()} as avg_price_per_m, "
        "   count(*) as num_flats "
        "FROM prices "
        f"INNER JOIN ({get_flats_db()}) as flats "
        "ON prices.flat_id = flats.ad_id "
        "GROUP BY location, month_num "
        f"HAVING price > 1000 and flat_area > 0 and flat_area < {max_area} ",
        conn)

    return df


def get_price_m_loc_area_cat(conn=None) -> pd.DataFrame:
    """
    Queries SQlite database, merges two tables and retrieves a DataFrame
    """
    df = pd.read_sql_query(
        "SELECT "
        "   location, "
        f" {get_area_categories()} ,"
        "   SUBSTR(date_scraped, 6,2) as month_num, "
        f"  {calc_avg_price()} as avg_price_per_m, "
        "   count(*) as num_flats "
        "FROM prices "
        f"INNER JOIN ({get_flats_db()}) as flats "
        "ON prices.flat_id = flats.ad_id "
        "GROUP BY location, month_num, area_category "
        f"HAVING price > 1000 and flat_area > 0 and flat_area < {max_area} ",
        conn)

    return df


def get_dates(conn=None) -> dict:
    df = pd.read_sql_query(
        "SELECT max(date_scraped) as max_date, min(date_scraped) as min_date "
        "FROM flats ",
        conn)

    datetimeobj_max = datetime.strptime(df.max_date[0], '%Y-%m-%d')
    max_date = datetimeobj_max.strftime('%B %d, %Y')

    datetimeobj_min = datetime.strptime(df.min_date[0], '%Y-%m-%d')
    min_date = datetimeobj_min.strftime('%B %d, %Y')

    return {"max_date": max_date, "min_date": min_date}
