#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


SOURCES = {
    "green_taxi_data_Nov_2025": {
        "url": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet",
        "kind": "parquet",
    },
    "taxi_zone_lookup": {
        "url": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv",
        "kind": "csv",
    },
}


def parquet_chunks(url, chunksize):
    df = pd.read_parquet(url)
    for start in range(0, len(df), chunksize):
        yield df.iloc[start:start + chunksize]


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading source data')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize):

    engine = create_engine(f'postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    for target_table, source in SOURCES.items():
        print(f"Loading {target_table} from {source['url']}")

        if source["kind"] == "parquet":
            df_iter = parquet_chunks(source["url"], chunksize)
        else:
            df_iter = pd.read_csv(
                source["url"],
                iterator=True,
                chunksize=chunksize
            )

        first = True

        for df_chunk in tqdm(df_iter):

            if first:
                # Create table schema (no data)
                df_chunk.head(0).to_sql(
                    name=target_table,
                    con=engine,
                    if_exists="replace"
                )
                first = False
                print("Table created:", target_table)

            # Insert chunk
            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="append"
            )

            print("Inserted:", len(df_chunk))

if __name__ == "__main__":
    run()
