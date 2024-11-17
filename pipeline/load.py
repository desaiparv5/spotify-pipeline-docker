import nats
import asyncio
import signal
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from io import StringIO
import os
from logger import logger


NATS_URL = os.environ.get("NATS_URL")
ARTISTS_QUEUE = os.environ.get("ARTISTS_QUEUE")
SONGS_QUEUE = os.environ.get("SONGS_QUEUE")
ALBUMS_QUEUE = os.environ.get("ALBUMS_QUEUE")
TRANSFORM_QUEUE = os.environ.get("TRANSFORM_QUEUE")

SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER") 
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA")

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

async def load_songs_function(msg):
    df = pd.read_csv(
        StringIO(msg.data.decode("utf-8")),
    )
    df.columns = [col.upper() for col in df.columns]
    success, nchunks, nrows, _ = write_pandas(
        conn=get_snowflake_connection(),
        df=df,
        table_name="SONG_DATA"
    )
    logger.info(f"Loaded {nrows} rows in SONG_DATA table")


async def load_artist_function(msg):
    df = pd.read_csv(
        StringIO(msg.data.decode("utf-8")),
    )
    df.columns = [col.upper() for col in df.columns]
    success, nchunks, nrows, _ = write_pandas(
        conn=get_snowflake_connection(),
        df=df,
        table_name="ARTISTS"
    )
    logger.info(f"Loaded {nrows} rows in ARTISTS table")


async def load_album_function(msg):
    df = pd.read_csv(
        StringIO(msg.data.decode("utf-8")),
    )
    df.columns = [col.upper() for col in df.columns]
    success, nchunks, nrows, _ = write_pandas(
        conn=get_snowflake_connection(),
        df=df,
        table_name="ALBUMS"
    )
    logger.info(f"Loaded {nrows} rows in ALBUMS table")


async def run(loop):
    nc = nats.aio.client.Client()

    # Add kill commands
    def signal_handler():
        if nc.is_closed:
            return
        logger.info("Disconnecting...")
        loop.create_task(nc.close())
    for sig in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, sig), signal_handler)

    async def error_cb(e):
        logger.info("Error:", e)

    async def closed_cb():
        logger.info("Connection to NATS is closed.")
        await asyncio.sleep(0.1)
        loop.stop()

    async def reconnected_cb():
        logger.info(f"Connected to NATS at {nc.connected_url.netloc}...")

    options = {
        "error_cb": error_cb,
        "closed_cb": closed_cb,
        "reconnected_cb": reconnected_cb,
        "servers": NATS_URL
    }
    await nc.connect(**options)
    await nc.subscribe(subject=SONGS_QUEUE, cb=load_songs_function, queue="load_queue")
    await nc.subscribe(subject=ARTISTS_QUEUE, cb=load_artist_function, queue="load_queue")
    await nc.subscribe(subject=ALBUMS_QUEUE, cb=load_album_function, queue="load_queue")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    try:
        loop.run_forever()
    finally:
        loop.close()
