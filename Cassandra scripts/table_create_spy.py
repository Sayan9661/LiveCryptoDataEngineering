from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
# This is the unused script for S&P 500.
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
CREATE TABLE IF NOT EXISTS spy_info (
    id uuid PRIMARY KEY,
    timestamp timestamp,
    open double,
    high double,
    low double,
    close double,
    volume double
);

"""

session.execute(create_table_query)
