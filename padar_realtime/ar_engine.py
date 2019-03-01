import time
import asyncio
from pymetawear.discover import discover_devices
from pymetawear.client import MetaWearClient
import websockets
import json
import functools
from threading import Thread
from multiprocessing import Process, Queue
import signal
import os
import pandas as pd
import time
import random
import concurrent.futures
from datetime import datetime


class ActivityRecognitionStream(object):
    def __init__(self,
                 loop,
                 data_queue,
                 host='localhost',
                 port=9000,
                 accel_sr=50,
                 update_rate=2):
        # Thread.__init__(self)
        self._host = host
        self._port = port
        self._accel_sr = accel_sr
        self._data_queue = data_queue
        self._result_queue = asyncio.Queue()
        self._clients = set()
        self._server = websockets.serve(self._ws_handler, host=host, port=port)
        self._loop = loop
        self._future_results = set()

    def set_ar_pipeline(self, pipeline):
        self._pipeline = pipeline

    def _sending_result(self, future):
        result = future.result()
        self._future_results.remove(future)
        print('Remaining jobs: ' + str(len(self._future_results)))
        # print('computed data: ' + str(result[0]['HEADER_TIME_STAMP']))
        print('data_queue size: ' + str(self._data_queue.qsize()))
        if len(self._clients) > 0:
            self._loop.call_soon_threadsafe(self._result_queue.put_nowait,
                                            result)

    async def _do_computing(self):
        with concurrent.futures.ProcessPoolExecutor() as pool:
            while True:
                data = await self._data_queue.get()
                print('received data: ' + str(data[0]['HEADER_TIME_STAMP']))
                print('data_queue size: ' + str(self._data_queue.qsize()))
                future_result = self._loop.run_in_executor(
                    pool, self._pipeline, data)
                future_result.add_done_callback(self._sending_result)
                self._future_results.add(future_result)
                print('Running jobs: ' + str(len(self._future_results)))

    def run(self):
        print('Waiting for incoming data...')
        return self._do_computing()

    def run_ws(self, loop):
        print('Start ws server on: ' + self._host + ':' + str(self._port))
        loop.run_until_complete(self._server)

    async def result_stream(self):
        while True:
            value = await self._result_queue.get()
            yield value

    async def _ws_handler(self, client, path):
        print('ws client is added')
        self._clients.add(client)
        try:
            async for value in self.result_stream():
                for client in self._clients:
                    await client.send(value)
        except websockets.exceptions.ConnectionClosed:
            print('ws client disconnected')
            self._clients.remove(client)
        finally:
            print('remaining clients: ' + str(len(self._clients)))


class ActivityRecognitionStreamManager(object):
    def __init__(self,
                 loop,
                 n_data_sources=2,
                 init_input_port=8000,
                 init_output_port=9000):
        self._loop = loop
        self._n_data_sources = n_data_sources
        self._init_input_port = init_input_port
        self._init_output_port = init_output_port
        self._pipelines = []
        self._data_queues = []

    def add_ar_stream(self, pipeline_func):
        self._pipelines.append(pipeline_func)
        self._data_queues.append(asyncio.Queue())
        self._output_port = len(self._pipelines) - 1 + self._init_output_port
        return self

    def start_simulation(self):
        def _simulate():
            while True:
                data_buffer = []
                for i in range(50 * 5):
                    data = {
                        'HEADER_TIME_STAMP':
                        datetime.fromtimestamp(
                            time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                        'X':
                        random.random(),
                        'Y':
                        random.random(),
                        'Z':
                        random.random()
                    }
                    data_buffer.append(data)
                for data_queue in self._data_queues:
                    self._loop.call_soon_threadsafe(data_queue.put_nowait,
                                                    data_buffer)
                time.sleep(8)

        Thread(target=_simulate).start()

    def start(self, ws_server=True):
        for pipeline, data_queue in zip(self._pipelines, self._data_queues):
            stream = ActivityRecognitionStream(
                loop,
                data_queue,
                host='localhost',
                port=self._output_port,
                accel_sr=50,
                update_rate=2)
            stream.set_ar_pipeline(pipeline)
            self._loop.create_task(stream.run())
            stream.run_ws(self._loop)
        self._loop.run_forever()


def pipeline(data):
    time.sleep(10)
    return json.dumps(data)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    loop = asyncio.get_event_loop()
    stream_manager = ActivityRecognitionStreamManager(
        loop=loop, n_data_sources=2, init_output_port=9000)
    stream_manager.add_ar_stream(pipeline)
    stream_manager.start_simulation()
    stream_manager.start(ws_server=True)
