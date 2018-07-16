from padar_realtime.libs.websocket_server_child_process import WebsocketServerChildProcess
import asyncio
import logging
import sys
from multiprocessing import Process

logger = logging.getLogger()

async def test_producer(queue):
    logging.info('Queue size:' + str(queue.qsize()))
    while queue.empty():
      await asyncio.sleep(1)
    data = queue.get()
    logging.info('Get data from queue: ' + str(data))
    return data

if __name__ == '__main__':
    import sys
    import time
    
    from random import random

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    server = WebsocketServerChildProcess()
    server.setup_producer(test_producer, desc='echo function')
    server.start()
    n = 0
    while n < 1000:
        time.sleep(5)
        data = random()
        logging.info("Making data every second: " + str(data))
        server.send_to_server(data)
