import signal
from padar_realtime.input_stream import InputStream
import asyncio
import time


async def display(stream):
    print('Waiting for input stream...')
    async for chunk in stream.get_chunk_stream():
        print(chunk)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    loop = asyncio.get_event_loop()
    window_size = 12
    update_rate = 1
    stream1 = InputStream(
        loop,
        name='stream1',
        host='localhost',
        port=8000,
        window_size=window_size,
        update_rate=update_rate,
        session_st=time.time())
    stream2 = InputStream(
        loop,
        name='stream2',
        host='localhost',
        port=8001,
        window_size=window_size,
        update_rate=update_rate,
        session_st=time.time())
    stream1.start()
    stream2.start()
    loop.create_task(display(stream1))
    loop.create_task(display(stream2))
    loop.run_forever()