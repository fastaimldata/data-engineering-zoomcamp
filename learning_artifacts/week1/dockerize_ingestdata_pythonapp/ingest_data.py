#!/usr/bin/env python
# coding: utf-8

# Run as container
# export URL=https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv
# docker run -it --network=dockerized_postgres_pgadmin_postgres-network ingestdata --host=postgresql_service --url=${URL}  --table_name yellow_taxi_data
# export URL=https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
# docker run -it --network=dockerized_postgres_pgadmin_postgres-network ingestdata --host=postgresql_service --url=${URL} --table_name taxi_zone_lookup

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine

def ingest_small_csv(params):
    
    print('Ingesting zones csv')

    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    csv_name = 'output.csv'

    # Download the csv file directly with wget
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    t_start = time()
    df = pd.read_csv(csv_name)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='replace')
    t_end = time()

    print('Ingested zones csv, took %.3f seconds' % (t_end - t_start))


def ingest_yellow_taxi_data(params):
    
    print('Ingesting taxi data csv')

    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    csv_name = 'output.csv'

    # Download the csv file directly with wget
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime  = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 
        t_start = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

def main(params):
    if 'taxi+_zone_lookup.csv' in params.url:
        ingest_small_csv(params)
    elif 'yellow_tripdata_2021-01.csv' in params.url:
        ingest_yellow_taxi_data(params)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres (see docker-compose.yaml to understand the options)',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--user', default='root', help='user name for postgres')
    parser.add_argument('--password', default='admin', help='password for postgres')
    parser.add_argument('--host', default='postgresql_service', help='Name of the postgresql docker service. Should not be localhost because for the container localhost is itself')
    parser.add_argument('--port', default='5432', help='port for postgres')
    parser.add_argument('--db', default='ny_taxi', help='database name for postgres')
    parser.add_argument('--table_name', default='yellow_taxi_data', help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)