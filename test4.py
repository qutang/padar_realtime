import asyncio
import random
import time
from threading import Thread

class NeedCallback(Thread):
    def __init__(self, callback):
        Thread.__init__(self)
        self.callback=callback

    def run(self):
        data = random.random()
        self.callback(data)
        time.sleep(0.5)

async def yield_num2(queue):
    loop = asyncio.get_event_loop()
    def put(data): 
        loop.call_soon_threadsafe(queue.put_nowait, data)
    thread = NeedCallback(put)
    thread.start()

class AsyncIterable:
    def __init__(self, queue):
        self.queue = queue
        self.done = []

    def __aiter__(self):
        return self

    async def __anext__(self):
        data = await self.fetch_data()
        if data is not None:
            return data
        else:
            raise StopAsyncIteration

    async def fetch_data(self):
        while not self.queue.empty():
            self.done.append(self.queue.get_nowait())
        if not self.done:
            return None
        return self.done.pop(0)


async def consume_num(queue):
    async for i in AsyncIterable(queue):
        await asyncio.sleep(0.5)
        print(i)


def main():
    event_loop = asyncio.get_event_loop()
    queue = asyncio.Queue(loop=event_loop)
    try:
        event_loop.create_task(yield_num2(queue))
        event_loop.run_until_complete(consume_num(queue))
    finally:
        event_loop.close()


if __name__ == '__main__':
    main()