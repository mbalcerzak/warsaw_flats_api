# Query SQLite database and get pandas DataFrames
import pandas as pd


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
        "WHERE flat_area > 0"
        )


def calc_avg_price():
    return "ROUND((prices.price/flat_area),2)"


def load_df(conn=None, n=1500) -> pd.DataFrame:
    """
    Queries SQlite database, merges two tables and retrieves a DataFrame
    """
    query_ = (f"SELECT * , {get_area_categories()} "
              "FROM flats "
              "WHERE flat_area > 0 ")
    dfs = []

    chunk_num = 0
    print(len(pd.read_sql_query(query_, con=conn, chunksize=n)))
    for chunk in pd.read_sql_query(query_, con=conn, chunksize=n):
        dfs.append(chunk)
        chunk_num += 1
        print("Chunk ", str(chunk_num))

    print("Concatenating chunks")

    df = pd.concat(dfs)
    df = df.reset_index()

    return df


def load_df_avg_prices(conn=None) -> pd.DataFrame:
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
        "GROUP BY location, month_num ",
        conn)

    return df


def load_area_cat_df(conn=None) -> pd.DataFrame:
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
        "GROUP BY location, month_num, area_category ",
        conn)

    return df
