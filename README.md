# LiveCryptoDataEngineering
![alt text](https://github.com/Sayan9661/LiveCryptoDataEngineering/blob/main/grafana_scn.jpg "Grafana Dash")


# Project Description
This is our project for NYU-CS 6513 Big Data. Our intention for this project is to build a big data pipline that creates a highly scalable system to pull data from online cryptocurrencies APIs using Kafka, transform the data using Spark, save it in our Cassandra database, and perform computation and live-updating visualization. 

To understand our pipline, if you are a NYU student/faculty you can checkout our presentation slides:
https://docs.google.com/presentation/d/1a31ZSwoWalOhHMRE-DMZHV_bYN83mCTpYGzosStfRjE/edit?usp=sharing

In this first iteration, we are visualizing the price of 4 famous cryptocurrencies: Bitcoin, Ethereum, Solana, Dogecoin.

The visualization product is powered by Knowi and Grafana, checkout here for our Knowi dashboard:

https://www.knowi.com/d/E19ii0ripipLedrFLUbN1wh44lisFge40lBg1sDMOqKtZkUie

The visualization should be updating live in 1-minute interval, indicating that the pipline is functioning. 

Grafana is our dockerzied version of visualization tool (running on Google Cloud). To see the output, login using username: `BigData` and password `bigdata2023` and then access the link:

http://35.232.134.197:3000/d/b629c4e8-dd84-43cb-94cd-640d322be4ac/new-dashboard?orgId=1&refresh=10s

We plan on adding more features later. 

# Environment Setup
Our pipline is functioning entirely on Google Cloud and each module is fully containerized, with the exception being Cassandra-db which is native on cloud.

If you want to deploy each module locally, use `docker compose up` when your `cwd` is inside of our docker folder where the `.yml` files exists.

Note: If you deploy each module locally, `userdb` and `dbpass` are the environment variables that need to be set to connect to our cloud database. Contact us if you are one of the NYU students/faculties and want to connect to our database.

## Repo Contents:

### API Scripts:

`producer_bitcoin.py` - API Endpoint Scripts which ingests data form API calls to kafka for Bitcoin data.<br>
`producer_ethereum.py` - API Endpoint Scripts which ingests data form API calls to kafka for Ethereum data.<br>
`producer_solana.py` - API Endpoint Scripts which ingests data form API calls to kafka for Solana data.<br>
`producer_dogecoin.py` - API Endpoint Scripts which ingests data form API calls to kafka for Dogecoin data.<br>

### Kafka Code:

`consumer.py` - Code for testing kafka.

### Spark Code:

`main.py` - Performs our ETL flow. Transforms the ingested data from kafka and api calls and sends it to Cassandra.

### Cassandra Scripts(DO NOT RUN):

`table_create_bitcoin.py` - Script to create bitcoin table on the databases for Bitcoin. <br>
`table_create_ether.py` - Script to create ethereum table on the databases for Ethereum. <br>
`table_create_solana.py` - Script to create solana table on the databases for Solana. <br>
`table_create_spy.py` - Unsed File for S&P 500 ETF (due to us not able to afford prime account :( ) .<br>
`test_insert_bitcoin.py` - Script to inject bitcoin data info into databases from api. For testing purpose only. <br>
`test_insert_dogecoin.py` - Script to inject dogecoin data into databases from api. For testing purpose only. <br>
`test_insert_ether.py` - Script to inject ethereum data into databases from api. For testing purpose only.<br>
`test_insert_solana.py` - Script to inject solana data into databases from api. For testing purpose only. <br>

### Spark, Kafka and Grafana folders contain the docker-compose-yml files to host the tech stack on Docker.




