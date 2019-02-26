import random
import time
import os
import asyncio
from threading import Thread
from multiprocess import Process

class NeedCallback(Thread):
    def __init__(self, callback):
        Thread.__init__(self)
        self.callback=callback

    def run(self):
        while True:
          data = random.random()
          self.callback(data)
          time.sleep(0.1)
  
class NeedCallback2(Thread):
    def __init__(self, callback):
        Thread.__init__(self)
        self.callback=callback

    def run(self):
        while True:
          data = random.randint(0, 10)
          self.callback(data)
          time.sleep(0.1)

def make_iter():
    loop = asyncio.get_event_loop()
    queue = asyncio.Queue()
    def put(data):
        loop.call_soon_threadsafe(queue.put_nowait, data)
    async def get():
        while True:
            value = await queue.get()
            yield value
    return get(), put

async def main():
    stream_get, stream_put = make_iter()
    stream = NeedCallback(callback=stream_put)
    stream2 = NeedCallback2(callback=stream_put)
    stream.start()
    stream2.start()
    async for data in stream_get:
        print(data)

if __name__ == '__main__':
  # need_callback(callback)
  asyncio.get_event_loop().run_until_complete(main())
  # NeedCallback(lambda d: print(d)).start()