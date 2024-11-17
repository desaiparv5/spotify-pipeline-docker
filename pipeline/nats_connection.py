import nats
import nats.aio
import nats.aio.client

class NatsConnection:
    def __init__(self, url):
        self.url = url
        self._connection = None

    async def publish(self, subject, data):
        await self.connection.publish(subject, data)

    async def __aenter__(self):
        if not self._connection:
            self._connection = await nats.connect(self.url)
        return self._connection

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        await self._connection.close()
