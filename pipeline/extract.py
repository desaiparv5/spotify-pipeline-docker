import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import nats_connection
import asyncio
from logger import logger


NATS_URL = os.environ.get("NATS_URL")
TRANSFORM_QUEUE = os.environ.get("TRANSFORM_QUEUE")
cilent_id = os.environ.get('SPOTIFY_CLIENT_ID')
client_secret = os.environ.get('SPOTIFY_CLIENT_SECRET')
playlist_id = os.environ.get('PLAYLIST_ID')

async def main():
    logger.info("Extracting data...")

    client_credentials_manager = SpotifyClientCredentials(client_id=cilent_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

    spotify_data = sp.playlist_tracks(playlist_id)
    
    async with nats_connection.NatsConnection(NATS_URL) as nc:
        await nc.publish(TRANSFORM_QUEUE, payload=json.dumps(spotify_data).encode('utf-8'))
        logger.info("Published data to pipeline.")

if __name__ == "__main__":
    asyncio.run(main())
