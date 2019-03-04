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
                print('data_queue size: ' + str(self._data_queue.qsize()))
                future_result = self._loop.run_in_executor(
                    pool, self._pipeline, data)
                future_result.add_done_callback(self._sending_result)
                self._future_results.add(future_result)
                print('Running jobs: ' + str(len(self._future_results)))

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

    def add_input_stream(self, name, host='*'):
        stream = InputStream(
            self._loop,
            host=host,
            port=len(self._input_streams) + self._init_input_port)
        self._input_streams[name] = {
            'stream': stream,
            'buffer': [],
            'update_pointer': 0
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
                streamer['buffer'].append(data)
                if streamer['update_pointer'] == 0:
                    streamer['update_pointer'] = data['HEADER_TIME_STAMP']
                elif (
                        streamer['buffer'][-1]['HEADER_TIME_STAMP'] -
                        streamer['buffer'][0]['HEADER_TIME_STAMP'] >=
                        self._window_size
                ) & (data['HEADER_TIME_STAMP'] - streamer['update_pointer'] >=
                     self._window_size - self._update_rate):
                    streamer['update_pointer'] = streamer[
                        'update_pointer'] + self._update_rate
                    package = copy.deepcopy({
                        'name': name,
                        'data': streamer['buffer']
                    })
                    for output_stream in self._output_streams:
                        output_stream.put_input_queue(package)
                    streamer['buffer'] = list(
                        filter(
                            lambda x: x['HEADER_TIME_STAMP'] >= streamer['update_pointer'],
                            streamer['buffer']))

    def start_input_streams(self):
        for _, streamer in self._input_streams.items():
            streamer['stream'].start()
            self._loop.create_task(self._input_handler())

    def start(self):
        self.start_input_streams()
        for stream in self._output_streams:
            stream.run()
            stream.run_ws()
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
