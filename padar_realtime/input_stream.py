import asyncio
import websockets
from threading import Thread
import signal


class InputStream(Thread):
    def __init__(self, loop, host, port):
        Thread.__init__(self)
        self._host = host
        self._port = port
        self._loop = loop
        self._queue = asyncio.Queue()

    async def _handler(self):
        try:
            async with websockets.connect('ws://' + self._host + ':' +
                                          str(self._port)) as websocket:
                while True:
                    data = await websocket.recv()
                    self._loop.call_soon_threadsafe(self._queue.put_nowait,
                                                    data)
        except Exception as e:
            print(str(e))

    async def get_stream(self):
        while True:
            data = await self._queue.get()
            yield data

    def run(self):
        self._loop.create_task(self._handler())


async def display(stream):
    print('Waiting for input stream...')
    async for data in stream.get_stream():
        print(data)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    loop = asyncio.get_event_loop()
    stream = InputStream(loop, host='wockets.ccs.neu.edu', port=8000)
    stream.start()
    loop.create_task(display(stream))
    loop.run_forever()
