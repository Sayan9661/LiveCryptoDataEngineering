from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
# WARNING: PLEASE DO NOT RUN THIS SCRIPT
# This is to document how to create table on the databases. It's not part of the pipline
# Running this script will casue issue to cassandra tables.
userdb = os.environ.get('userdb')
dbpass = os.environ.get('dbpass')

cloud_config = {
    'secure_connect_bundle': 'secure-connect-crypto-db.zip'
}
auth_provider = PlainTextAuthProvider('userdb', 'dbpass')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
keyspace = 'bigdata'
session = cluster.connect(keyspace)


create_table_query = """
CREATE TABLE IF NOT EXISTS solana_info (
    id UUID PRIMARY KEY,
    name text,
    symbol text,
    time_last_updated timestamp,
    current_price_usd double,
    block_time_in_minutes double,
    total_volume_usd double,
    market_cap_rank int,
    high_24h_usd double,
    low_24h_usd double,
    price_change_percentage_24h double,
    price_change_percentage_200d_usd double,
    price_change_percentage_7d_usd double,
    price_change_percentage_30d_usd double,
    price_change_percentage_60d_usd double,
    price_change_percentage_1y_usd double,
    ticker_base text,
    ticker_target text,
    market_name text,
    ticker_last double,
    ticker_volume double,
    converted_last_eth double,
    converted_volume_eth double,
    trust_score text
);
"""

session.execute(create_table_query)
