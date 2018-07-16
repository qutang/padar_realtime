"""
Computation engine based on websocket protocol

1. It uses websocket as data receiving and broadcasting socket
2. It uses child process for CPU heavy computation
3. It uses queue (thread-safe) for data buffering

Author: Qu Tang
Date: Jul 05, 2018
"""
from .libs.websocket_server_child_process import WebsocketServerChildProcess
from .libs.websocket_client import WebsocketClient
from functools import partial
import logging
import time
import pandas as pd
import asyncio
import json
import copy

logger = logging.getLogger()


class ComputationEngine:
    def __init__(self, b_url='localhost', b_port=1111, r_url='localhost', r_port=2222, computation_interval=12.8):
        self._b_url = b_url
        self._b_port = b_port
        self._r_url = r_url
        self._r_port = r_port
        self._receiver = WebsocketClient(self._r_url, self._r_port)
        self._sender = WebsocketServerChildProcess(self._b_url, self._b_port)
        self._input_buffer = []
        self._cycle_counter = 0
        self._last_send = 0
        self._interval = computation_interval

    def setup_computation_function(self, func, desc, **kwargs):
        self._func = partial(func, **kwargs)
        self._desc = desc
        return self

    @staticmethod
    def convert_json_to_dataframe(data_in_json_list):
        data = pd.read_json(data_in_json_list, orient='records')
        return data

    @staticmethod
    def convert_dataframe_to_json(df):
        json_list = df.to_json(orient='records')
        return json_list

    @staticmethod
    async def producer(queue, func):
        logger.debug('Producer Queue size:' + str(queue.qsize()))
        while queue.empty():
            await asyncio.sleep(1)
        data = queue.get()
        logger.info('Get data from queue with size: ' + str(len(data)))
        data = pd.DataFrame(data)
        result = func(data)
        message = ComputationEngine.convert_dataframe_to_json(result)
        return message

    def start(self):
        async def consumer(message):
            message = json.loads(message)
            self._input_buffer.append(message)
            current_ts = time.time()
            if self._last_send == 0:
                self._last_send = current_ts
            elif current_ts - self._last_send > self._interval:
                logger.info(
                    "Send to computation child process, data chunk size: " + str(len(self._input_buffer)))
                data = copy.deepcopy(self._input_buffer)
                self._sender.send_to_server(data)
                self._input_buffer.clear()
                self._last_send = current_ts

        self._sender.setup_producer(
            producer_func=ComputationEngine.producer, desc=self._desc, func=self._func).start()
        self._receiver.make_consumer(consumer=consumer).start()
        return self
