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
from .stream_package import SensorStreamPackage, SensorStreamChunk
from collections import deque


class ChunkMerger(object):
    def __init__(self, data_type, sync_st, n_streams, update_rate):
        self._data_type = data_type
        self._chunk_buffer = {}
        self._next_chunk_st = None
        self._merged = []
        self._n_streams = n_streams
        self._update_rate = update_rate
        self._merged_chunk_index = 0

    def process_chunk(self, chunk, send_callback):
        result = None
        self._add_chunk(chunk)
        self._merged += self._look_in_buffer(self._next_chunk_st)
        if self._is_time_to_merge(chunk):
            result = self._merge()
            send_callback(result)
            # next merge
            self._next_chunk_st = self._next_chunk_st + self._update_rate

    def _look_in_buffer(self, st):
        matched = []
        for id, buffer in self._chunk_buffer.items():
            while len(buffer) > 0 and buffer[0].get_chunk_st() < st:
                print('Buffer should not contain data from the past')
                buffer.popleft()
            if len(buffer) > 0 and buffer[0].get_chunk_st == st:
                matched.append(buffer.popleft())
        return matched

    def _add_chunk(self, chunk):
        if self._next_chunk_st is None:
            # create a new merge
            self._next_chunk_st = chunk.get_chunk_st()
            self._merged.append(chunk)
        else:
            if chunk.get_chunk_st() < self._next_chunk_st:
                print('discard this chunk from past from merger')
            elif chunk.get_chunk_st() == self._next_chunk_st:
                self._merged.append(chunk)
            else:
                self._add_to_buffer(chunk)

    def _add_to_buffer(self, chunk):
        id = chunk.get_device_id()
        if id in self._chunk_buffer:
            self._chunk_buffer[id].append(chunk)
        else:
            self._chunk_buffer[id] = deque()
            self._chunk_buffer[id].append(chunk)

    def _is_time_to_merge(self, chunk):
        id = chunk.get_device_id()
        existing_ids = list(map(lambda x: x.get_device_id, self._merged))
        # if we have data from every stream
        if len(self._merged) == self._n_streams:
            print('condition 1')
            return True
        # if we have waited for more than two chunks from the same stream
        elif id in self._chunk_buffer and len(
                self._chunk_buffer[id]) > 2 and len(self._merged) > 0:
            print('condition 2')
            return True
        return False

    def _merge(self):
        result = {
            'DATA_TYPE': self._merged[0].get_data_type(),
            'N_STREAMS': len(self._merged),
            'CHUNKS': copy.deepcopy(self._merged),
            'MERGED_CHUNK_ST': self._merged[0].get_chunk_st(),
            'MERGED_CHUNK_ET': self._merged[0].get_chunk_et(),
            'MERGED_CHUNK_INDEX': self._merged_chunk_index
        }
        self._merged_chunk_index += 1
        self._merged.clear()
        return result


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
        # if len(self._clients) > 0:
        if self._allow_ws:
            self._loop.call_soon_threadsafe(self._result_queue.put_nowait,
                                            result)
            self._loop.call_soon_threadsafe(
                print, 'result_queue size: ' + str(self._result_queue.qsize()))

    async def _do_computing(self):
        sleep_time = 2
        with concurrent.futures.ProcessPoolExecutor() as pool:
            while True:
                data = await self._data_queue.get()
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
                json_result = value.to_json(orient='records')
                print(json_result)
                for client in self._clients:
                    await client.send(json_result)
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
        self._sync_st = None
        self._merger_manager = {}

    def add_input_stream(self, name, host='*'):
        stream = InputStream(
            self._loop,
            host=host,
            port=len(self._input_streams) + self._init_input_port)
        self._input_streams[name] = {'stream': stream, 'chunk_manager': {}}

    def add_processor_stream(self, pipeline_func, host='*', ws_server=True):
        output_stream = ProcessorStream(
            loop=self._loop,
            allow_ws=ws_server,
            host=host,
            port=len(self._output_streams) + self._init_output_port)
        output_stream.set_processor_pipeline(pipeline_func)
        self._output_streams.append(output_stream)
        return self

    def _handle_input_stream(self, input_package, chunk_manager):
        data_type = input_package.get_data_type()
        stream_name = input_package.get_stream_name()
        device_id = input_package.get_device_id()
        if data_type not in chunk_manager:
            chunk = SensorStreamChunk(stream_name, device_id, data_type)
        else:
            chunk = chunk_manager[data_type]

        # set start time of current chunk
        self.__set_chunk_st(chunk, input_package)

        # set stop time of current chunk
        self.__set_chunk_et(chunk)

        # create mergers
        if data_type not in self._merger_manager:
            self._merger_manager[data_type] = ChunkMerger(
                data_type,
                self._sync_st,
                n_streams=len(self._input_streams),
                update_rate=self._update_rate)
        merger = self._merger_manager[data_type]

        # send chunk
        self.__set_package_and_send_chunk(chunk, input_package, merger)

        chunk_manager[data_type] = chunk

    def __set_chunk_st(self, chunk, input_package):
        if chunk.get_last_chunk() is None:
            if chunk.get_chunk_st() is None:
                chunk.set_chunk_st(math.floor(input_package.get_timestamp()))
                if self._sync_st is None:
                    self._sync_st = chunk.get_chunk_st()
                else:
                    chunk.set_chunk_st(self._sync_st)
        else:
            if chunk.get_chunk_st() is None:
                chunk.set_chunk_st(chunk.get_last_chunk().get_chunk_st() +
                                   self._update_rate)

    def __set_chunk_et(self, chunk):
        chunk.set_chunk_et(chunk.get_chunk_st() + self._window_size)

    def __send_chunk(self, merged_chunks):
        for output_stream in self._output_streams:
            output_stream.put_input_queue(merged_chunks)

    def __set_package_and_send_chunk(self, chunk, input_package, merger):
        if input_package.get_timestamp() <= chunk.get_chunk_st():
            # when the input data has a timestamp from the past, out-of-date data
            print('Discard samples with earlier timestamps')
            return
        elif input_package.get_timestamp() > chunk.get_chunk_st(
        ) and input_package.get_timestamp() <= chunk.get_chunk_et():
            # when the input data belongs to current chunk
            chunk.add_package(input_package)
        else:  # input_package.get_timestamp() >= chunk.get_chunk_et()
            # when the input data belongs to the next chunk
            chunk.clear_last_chunk()
            copied_chunk = copy.deepcopy(chunk)

            # print('Sending ' + copied_chunk.get_data_type() + ' chunk ' +
            #       str(copied_chunk.get_chunk_index()) + ' from ' +
            #       copied_chunk.get_stream_name() + ' of device: ' +
            #       copied_chunk.get_device_id() + ' to merger')

            merger.process_chunk(copied_chunk, self.__send_chunk)

            # keep only partial of current chunk for the next turn
            remaining_packages = list(
                filter(
                    lambda p: p.get_timestamp() > chunk.get_chunk_st() + self._update_rate,
                    chunk.get_packages()))

            # reset chunk status
            chunk.set_last_chunk(copied_chunk)
            chunk.set_packages(remaining_packages)
            chunk.set_chunk_st(None)
            chunk.set_chunk_et(None)

    def _create_input_package(self, stream_name, value):
        input_package = SensorStreamPackage()
        input_package.from_json_string(value)
        input_package.set_stream_name(stream_name)
        return input_package

    async def _input_handler(self, name, streamer):
        async for value in streamer['stream'].get_stream():
            input_package = self._create_input_package(name, value)
            self._handle_input_stream(input_package, streamer['chunk_manager'])

    def start_input_streams(self):
        for name, streamer in self._input_streams.items():
            streamer['stream'].start()
            self._loop.create_task(self._input_handler(name, streamer))

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
