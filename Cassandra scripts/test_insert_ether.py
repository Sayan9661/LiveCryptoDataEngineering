import urllib.request
import json
import time

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os

# WARNING: PLEASE DO NOT RUN THIS SCRIPT
# This is to document how to inject data into databases from api. It's not part of the pipline
# and is for testing purposes. Running this script will casue issue to cassandra tables.
userdb = os.environ.get('userdb')
dbpass = os.environ.get('dbpass')

cloud_config = {
    'secure_connect_bundle': 'secure-connect-crypto-db.zip'
}
auth_provider = PlainTextAuthProvider('userdb', 'dbpass')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
keyspace = 'bigdata'
session = cluster.connect(keyspace)

insert_q = """INSERT INTO bigdata.ether_info (id, name,
    symbol,
    time_last_updated,
    description,
    current_price_usd,
    block_time_in_minutes,
    total_volume_usd,
    market_cap_rank,
    high_24h_usd,
    low_24h_usd,
    price_change_percentage_24h,
    price_change_percentage_200d_usd,
    price_change_percentage_7d_usd,
    price_change_percentage_30d_usd,
    price_change_percentage_60d_usd,
    price_change_percentage_1y_usd,
    ticker_base,
    ticker_target,
    market_name,
    ticker_last,
    ticker_volume,
    converted_last_eth,
    converted_volume_eth,
    trust_score)
    VALUES (uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

while True:
    url = 'https://api.coingecko.com/api/v3/coins/ethereum?localization=false&tickers=true&market_data=true&community_data=false&developer_data=false&sparkline=false'
    response = urllib.request.urlopen(url)
    data = response.read()
    data_js = json.loads(data)
    print(data_js)
    session.execute(
        insert_q,
        (
            data_js['name'],
            data_js['symbol'],
            data_js["market_data"]["last_updated"],
            data_js['description']['en'],
            data_js['market_data']['current_price']['usd'],
            data_js['block_time_in_minutes'],
            data_js['market_data']['total_volume']['usd'],
            data_js['market_data']['market_cap_rank'],
            data_js['market_data']['high_24h']['usd'],
            data_js['market_data']['low_24h']['usd'],
            data_js['market_data']['price_change_percentage_24h'],
            data_js['market_data']['price_change_percentage_200d_in_currency']['usd'],
            data_js['market_data']['price_change_percentage_7d_in_currency']['usd'],
            data_js['market_data']['price_change_percentage_30d_in_currency']['usd'],
            data_js['market_data']['price_change_percentage_60d_in_currency']['usd'],
            data_js['market_data']['price_change_percentage_1y_in_currency']['usd'],
            data_js['tickers'][0]['base'],
            data_js['tickers'][0]['target'],
            data_js['tickers'][0]['market']['name'],
            data_js['tickers'][0]['last'],
            data_js['tickers'][0]['volume'],
            data_js['tickers'][0]['converted_last']['eth'],
            data_js['tickers'][0]['converted_volume']['eth'],
            data_js['tickers'][0]['trust_score']
        )
    )
    time.sleep(60)
