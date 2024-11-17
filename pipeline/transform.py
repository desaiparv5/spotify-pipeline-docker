import json
from io import StringIO
import nats.aio
import nats.aio.client
import pandas as pd
import nats
import asyncio
import signal
import os
from logger import logger
import nats_connection


NATS_URL = os.environ.get("NATS_URL")
ARTISTS_QUEUE = os.environ.get("ARTISTS_QUEUE")
SONGS_QUEUE = os.environ.get("SONGS_QUEUE")
ALBUMS_QUEUE = os.environ.get("ALBUMS_QUEUE")
TRANSFORM_QUEUE = os.environ.get("TRANSFORM_QUEUE")


def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id':album_id,'name':album_name,'release_date':album_release_date,
                            'total_tracks':album_total_tracks,'url':album_url}
        album_list.append(album_element)
    return album_list

def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict = {'artist_id':artist['id'], 'artist_name':artist['name'], 'external_url': artist['href']}
                    artist_list.append(artist_dict)
    return artist_list

def songs(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                        'artist_id':artist_id
                       }
        song_list.append(song_element)

    return song_list

async def transform_function(message):
    logger.info("TRANSFORMING...")
    data = json.loads(message.data)
    album_list = album(data)
    artist_list = artist(data)
    song_list = songs(data)

    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(subset=['album_id'])

    artist_df = pd.DataFrame.from_dict(artist_list)
    artist_df = artist_df.drop_duplicates(subset=['artist_id'])

    #Song Dataframe
    song_df = pd.DataFrame.from_dict(song_list)

    album_df['release_date'] = pd.to_datetime(album_df['release_date'])
    song_df['song_added'] =  pd.to_datetime(song_df['song_added'])
    song_buffer=StringIO()
    song_df.to_csv(song_buffer, index=False)
    song_content = song_buffer.getvalue()
    logger.info(f"Songs transformed {len(song_df.index)}")
    async with nats_connection.NatsConnection(NATS_URL) as nc:
        await nc.publish(SONGS_QUEUE, song_content.encode())
        logger.info(f"Songs sent to queue {len(song_df.index)}")

    album_buffer=StringIO()
    album_df.to_csv(album_buffer, index=False)
    album_content = album_buffer.getvalue()
    logger.info(f"Albums transformed {len(song_df.index)}")
    async with nats_connection.NatsConnection(NATS_URL) as nc:
        await nc.publish(ALBUMS_QUEUE, album_content.encode())
        logger.info(f"Albums sent to queue {len(song_df.index)}")

    artist_buffer=StringIO()
    artist_df.to_csv(artist_buffer, index=False)
    artist_content = artist_buffer.getvalue()
    logger.info(f"Artists transformed {len(song_df.index)}")
    async with nats_connection.NatsConnection(NATS_URL) as nc:
        await nc.publish(ARTISTS_QUEUE, artist_content.encode())
        logger.info(f"Artists sent to queue {len(song_df.index)}")


async def run(loop):
    nc = nats.aio.client.Client()

    # Add kill commands
    def signal_handler():
        if nc.is_closed:
            return
        print("Disconnecting...")
        loop.create_task(nc.close())
    for sig in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, sig), signal_handler)

    async def error_cb(e):
        print("Error:", e)

    async def closed_cb():
        print("Connection to NATS is closed.")
        await asyncio.sleep(0.1)
        loop.stop()

    async def reconnected_cb():
        print(f"Connected to NATS at {nc.connected_url.netloc}...")

    options = {
        "error_cb": error_cb,
        "closed_cb": closed_cb,
        "reconnected_cb": reconnected_cb,
        "servers": NATS_URL
    }
    await nc.connect(**options)
    await nc.subscribe(subject=TRANSFORM_QUEUE, cb=transform_function, queue="transform_queue")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    try:
        loop.run_forever()
    finally:
        loop.close()
