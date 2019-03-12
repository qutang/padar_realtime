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
from .input_stream import InputStream
import json
import copy
import math


class ProcessorStream(object):
    def __init__(self, loop, allow_ws=True, host='*', port=9000):
        self._host = host
        self._port = port
        self._data_queue = asyncio.Queue()
        self._result_queue = asyncio.Queue()
        self._clients = set()
        self._server = websockets.serve(self._ws_handler, host=host, port=port)
        self._loop = loop
        self._future_results = set()
        self._allow_ws = allow_ws
        self._start = True

    def put_input_queue(self, data):
        self._loop.call_soon_threadsafe(self._data_queue.put_nowait, data)

    def get_input_queue_size(self):
        return self._data_queue.qsize()

    def set_processor_pipeline(self, pipeline):
        self._pipeline = {'func': pipeline, 'last_run': 0}

    def _sending_result(self, future):
        result = future.result()
        self._future_results.remove(future)
        # print('Remaining jobs: ' + str(len(self._future_results)))
        # print('computed data: ' + str(result[0]['HEADER_TIME_STAMP']))
        # print('data_queue size: ' + str(self._data_queue.qsize()))
        if len(self._clients) > 0:
            self._loop.call_soon_threadsafe(self._result_queue.put_nowait,
                                            result)

    async def _do_computing(self):
        sleep_time = 2
        with concurrent.futures.ProcessPoolExecutor() as pool:
            while True:
                data = await self._data_queue.get()
                print(data['data'][0])
                current_ts = time.time()
                print(current_ts - self._pipeline['last_run'])
                if self._pipeline['last_run'] == 0:
                    self._pipeline['last_run'] = current_ts
                elif current_ts - self._pipeline['last_run'] < 1:
                    # prevent two executors running too close to each other, setting the interval to be 2 second for now
                    self._pipeline['last_run'] = current_ts + sleep_time
                    print('sleep for 2 second')
                    time.sleep(sleep_time)
                # print('data_queue size: ' + str(self._data_queue.qsize()))
                future_result = self._loop.run_in_executor(
                    pool, self._pipeline['func'], data)
                future_result.add_done_callback(self._sending_result)
                self._future_results.add(future_result)
                # print('Running jobs: ' + str(len(self._future_results)))

    def run(self):
        print('Waiting for incoming data...')
        self._loop.create_task(self._do_computing())

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
                 init_input_port=8000,
                 init_output_port=9000,
                 window_size=12.8,
                 update_rate=2):
        self._loop = loop
        self._init_input_port = init_input_port
        self._init_output_port = init_output_port
        self._input_streams = {}
        self._output_streams = []
        self._window_size = window_size
        self._update_rate = update_rate
        self._start = True
        self._package_count = 0

    def add_input_stream(self, name, host='*'):
        stream = InputStream(
            self._loop,
            host=host,
            port=len(self._input_streams) + self._init_input_port)
        self._input_streams[name] = {
            'stream': stream,
            'buffer': [],
            'last_package': [],
            'package_st': None,
            'package_et': None
        }

    def add_processor_stream(self, pipeline_func, host='*', ws_server=True):
        output_stream = ProcessorStream(
            loop=self._loop,
            allow_ws=ws_server,
            host=host,
            port=len(self._output_streams) + self._init_output_port)
        output_stream.set_processor_pipeline(pipeline_func)
        self._output_streams.append(output_stream)
        return self

    async def _input_handler(self):
        for name, streamer in self._input_streams.items():
            async for value in streamer['stream'].get_stream():
                data = json.loads(value)
                if len(streamer['last_package']) == 0:
                    if streamer['package_st'] is None:
                        streamer['package_st'] = math.floor(
                            data['HEADER_TIME_STAMP'])
                else:
                    if streamer['package_st'] is None:
                        streamer['package_st'] = streamer['last_package'][-1][
                            'HEADER_TIME_STAMP']

                streamer[
                    'package_et'] = streamer['package_st'] + self._window_size

                if data['HEADER_TIME_STAMP'] <= streamer['package_st']:
                    print('Discard samples with earlier timestamps')
                    continue
                elif data['HEADER_TIME_STAMP'] > streamer[
                        'package_st'] and data[
                            'HEADER_TIME_STAMP'] <= streamer['package_et']:
                    streamer['buffer'].append(data)
                else:  # data['HEADER_TIME_STAMP'] >= package_et
                    print(len(streamer['buffer']))
                    package = copy.deepcopy({
                        'name': name,
                        'data': streamer['buffer'],
                        'window_size': self._window_size,
                        'count': self._package_count
                    })
                    self._package_count = self._package_count + 1
                    for output_stream in self._output_streams:
                        output_stream.put_input_queue(package)

                    # save last package
                    streamer['last_package'] = copy.deepcopy(
                        streamer['buffer'])

                    # keep only partial of current package for the next turn
                    streamer['buffer'] = list(
                        filter(
                            lambda x: x['HEADER_TIME_STAMP'] > streamer['package_st'] + self._update_rate,
                            streamer['buffer']))
                    streamer['buffer'].append(data)
                    streamer['package_st'] = None
                    streamer['package_et'] = None

    def start_input_streams(self):
        for _, streamer in self._input_streams.items():
            streamer['stream'].start()
            self._loop.create_task(self._input_handler())

    def start(self):
        for stream in self._output_streams:
            stream.run()
            stream.run_ws()
        self.start_input_streams()
        self._loop.run_forever()

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


# def pipeline(data):
#     result = pd.DataFrame(data['data'])
#     result['HEADER_TIME_STAMP'] = result['HEADER_TIME_STAMP'].map(
#         lambda x: pd.Timestamp.fromtimestamp(x))
#     print(result)
#     return json.dumps(data)

# if __name__ == '__main__':
#     signal.signal(signal.SIGINT, signal.SIG_DFL)
#     loop = asyncio.get_event_loop()
#     stream_manager = ProcessorStreamManager(
#         loop=loop,
#         init_input_port=8000,
#         init_output_port=9000,
#         window_size=3,
#         update_rate=3)
#     stream_manager.add_processor_stream(
#         pipeline, host='localhost', ws_server=False)
#     stream_manager.add_processor_stream(
#         pipeline, host='localhost', ws_server=False)
#     # stream_manager.start_simulation()
#     stream_manager.add_input_stream(name='test', host='localhost')
#     stream_manager.start()
