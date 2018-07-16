import logging
from random import random
import functools
import json
from multiprocessing import Process, Queue
from .websocket_server import WebsocketServer


logger = logging.getLogger()

def echo():
    logger.info('In child process')


class WebsocketServerChildProcess:
    def __init__(self, url='localhost', port=8848):
        self._url = url
        self._port = port
        self._queue = Queue()

    def setup_producer(self, producer_func=None, desc="", **kwargs):
        self._producer_func = functools.partial(producer_func, **kwargs)
        self._desc = desc
        return self

    def start(self):
        self._process = Process(target=WebsocketServer.child_process_starter, args=(self._producer_func, self._desc, self._queue, self._url, self._port))
        self._process.start()
        logging.debug("Child process " + str(self._process.pid) + " is alive: " + str(self._process.is_alive()))
        return self

    def send_to_server(self, data):
        self._queue.put(data)
        logging.debug('Queue size in main process: ' + str(self._queue.qsize()))
        return self

    def stop(self):
        self._process.terminate()
        return self
