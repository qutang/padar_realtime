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


class ProcessorStream(object):
    def __init__(self,
                 loop,
                 allow_ws=True,
                 host='*',
                 port=9000,
                 accel_sr=50,
                 update_rate=2):
        self._host = host
        self._port = port
        self._accel_sr = accel_sr
        self._data_queue = asyncio.Queue()
        self._result_queue = asyncio.Queue()
        self._clients = set()
        self._server = websockets.serve(self._ws_handler, host=host, port=port)
        self._loop = loop
        self._future_results = set()
        self._allow_ws = allow_ws

    def put_input_queue(self, data):
        self._loop.call_soon_threadsafe(self._data_queue.put_nowait, data)

    def get_input_queue_size(self):
        return self._data_queue.qsize()

    def set_processor_pipeline(self, pipeline):
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

    def run_ws(self):
        if self._allow_ws:
            print('Start ws server on: ' + self._host + ':' + str(self._port))
            self._loop.run_until_complete(self._server)
        else:
            print('WS server is disabled')

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


class ProcessorStreamManager(object):
    def __init__(self,
                 loop,
                 n_data_sources=2,
                 init_input_port=8000,
                 init_output_port=9000):
        self._loop = loop
        self._n_data_sources = n_data_sources
        self._init_input_port = init_input_port
        self._init_output_port = init_output_port
        self._input_sources = []
        self._output_streams = []
        self._ws_servers = []

    def add_processor_stream(self,
                             pipeline_func,
                             accel_sr,
                             update_rate,
                             host='*',
                             ws_server=True):
        output_stream = ProcessorStream(
            loop=self._loop,
            allow_ws=ws_server,
            host=host,
            port=len(self._output_streams) + self._init_output_port,
            accel_sr=accel_sr,
            update_rate=update_rate)
        output_stream.set_processor_pipeline(pipeline_func)
        self._output_streams.append(output_stream)
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
                for stream in self._output_streams:
                    stream.put_input_queue(data_buffer)
                time.sleep(5)

        Thread(target=_simulate).start()

    def start(self):
        for stream in self._output_streams:
            self._loop.create_task(stream.run())
            stream.run_ws()
        self._loop.run_forever()


def pipeline(data):
    time.sleep(10)
    return json.dumps(data)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    loop = asyncio.get_event_loop()
    stream_manager = ProcessorStreamManager(
        loop=loop, n_data_sources=2, init_output_port=9000)
    stream_manager.add_processor_stream(
        pipeline,
        host='localhost',
        accel_sr=50,
        update_rate=0.2,
        ws_server=True)
    stream_manager.add_processor_stream(
        pipeline, host='localhost', accel_sr=50, update_rate=2, ws_server=True)
    stream_manager.start_simulation()
    stream_manager.start()
