import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    """Main process"""

    # Get params values
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    url_zone_lookup = params.url_zone_lookup
    lookup_table_name = params.lookup_table_name

    # The backup files are gzipped
    # It's important to keep the correct extension
    # for pandas to be able to open the files
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    
    # Download data
    os.system(f'wget {url} -O {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    df['lpep_pickup_datetime'] = pd.to_datetime(df.lpep_pickup_datetime)
    df['lpep_dropoff_datetime'] = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    
    df.to_sql(name=table_name, con=engine, if_exists='append')
    
    while True:

        try:
            t_start = time()
            df = next(df_iter)
            df['lpep_pickup_datetime'] = pd.to_datetime(df.lpep_pickup_datetime)
            df['lpep_dropoff_datetime'] = pd.to_datetime(df.lpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print('Inserted another chunk, took %.3f seconds' % (t_end - t_start))

        except StopIteration:    
            print('Finished ingesting data into Postgres')
            break

    # Lookup table
    csv_name = 'lookup.csv'
    os.system(f'wget {url_zone_lookup} -O {csv_name}')
    lookup_df = pd.read_csv(csv_name)
    lookup_df.to_sql(name=lookup_table_name, con=engine, if_exists='append')

    return None

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV to database')\

    parser.add_argument('--user', required=True, help='User name for Postgres DB')
    parser.add_argument('--password', required=True, help='Password for Postgres DB')
    parser.add_argument('--host', required=True, help='Host for Postgres DB')
    parser.add_argument('--port', required=True, help='Port for Postgres DB')
    parser.add_argument('--db', required=True, help='Database name for Postgres')
    parser.add_argument('--table_name', required=True, help='Name of table to create')
    parser.add_argument('--url', required=True, help='Url of CSV file')
    parser.add_argument('--url_zone_lookup', required=True, help='Url of zone lookup')
    parser.add_argument('--lookup_table_name', required=True, help='Lookup table name')

    args = parser.parse_args()

    main(args)