import json
import sqlite3
from datetime import date
from collections import defaultdict
import random
import yaml


def query_db(connection):
    cursor = connection.cursor()
    json_price_history = defaultdict(dict)
    json_ad_id_link = {}
    latest_changed_flat_id = []

    query_lastest_date = """SELECT DISTINCT date FROM prices ORDER BY date DESC LIMIT 3"""
    cursor.execute(query_lastest_date)
    date_lastest_change = [x[0] for x in cursor.fetchall()]

    print(date_lastest_change)

    query = """SELECT
                    ad_id,
                    price,
                    date,
                    page_address
                FROM
                    prices
                LEFT JOIN 
                    flats ON flats.flat_id = prices.flat_id
                WHERE ad_id in (
                    SELECT 
                        ad_id 
                    FROM 
                        prices 
                    LEFT JOIN 
                        flats on flats.flat_id = prices.flat_id 
                    GROUP BY 
                        prices.flat_id 
                    HAVING 
                        count(prices.flat_id ) > 1
                ) 
            """

    cursor.execute(query)
    prices = cursor.fetchall()

    for row in prices:
        flat_id = str(row[0])
        price = row[1]
        price_date = row[2]
        page_address = row[3]

        json_price_history[flat_id][price_date] = price
        json_ad_id_link[flat_id] = page_address

        if flat_id not in latest_changed_flat_id and price_date in date_lastest_change:
            latest_changed_flat_id.append(flat_id)

    random_pick_ids = random.choices(latest_changed_flat_id, k=3)

    latest_changed_flats = {k:v for k,v in json_price_history.items() if k in random_pick_ids}
    random_links = {v:k for k,v in json_ad_id_link.items() if k in random_pick_ids}

    return json_price_history, latest_changed_flats, random_links


if __name__ == "__main__":
    with open(r'config.yaml') as f:
        paths = yaml.safe_load(f)

    data_path = paths['data_path']

    try:
        connection = sqlite3.connect(data_path, check_same_thread=False)
        connection.close()
        connection = sqlite3.connect(data_path, check_same_thread=False)
    
        _, latest_changes, random_links = query_db(connection)

        with open('json_dir/latest_changes.json', 'w') as f:
            json.dump(latest_changes, f)
        
        with open('json_dir/random_links.json', 'w') as f:
            json.dump(random_links, f)

    except sqlite3.Error as e:
        raise Exception

    connection.close()

