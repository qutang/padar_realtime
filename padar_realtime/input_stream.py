import asyncio
import websockets
from threading import Thread
from .stream_package import SensorStreamPackage, SensorStreamChunk
import math
import copy


class InputStream(Thread):
    def __init__(self,
                 loop,
                 name,
                 host,
                 port,
                 window_size,
                 update_rate,
                 session_st=None):
        Thread.__init__(self)
        self._host = host
        self._port = port
        self._loop = loop
        self._queue = asyncio.Queue()
        self._window_size = window_size
        self._update_rate = update_rate
        self._name = name
        self._chunk_manager = {}
        self._session_st = session_st

    def set_session_st(self, session_st):
        self._session_st = session_st

    def _create_input_package(self, value):
        input_package = SensorStreamPackage()
        input_package.from_json_string(value)
        input_package.set_stream_name(self._name)
        return input_package

    async def _ws_handler(self):
        try:
            async with websockets.connect('ws://' + self._host + ':' +
                                          str(self._port)) as websocket:
                while True:
                    data = await websocket.recv()
                    self._handle_input_stream(data)
        except Exception as e:
            print(str(e))

    async def get_chunk_stream(self):
        while True:
            chunk = await self._queue.get()
            yield chunk

    def _handle_input_stream(self, data):
        package = self._create_input_package(data)
        data_type = package.get_data_type()
        device_id = package.get_device_id()
        if data_type not in self._chunk_manager:
            chunk = SensorStreamChunk(self._name, device_id, data_type)
        else:
            chunk = self._chunk_manager[data_type]

        # set start time of current chunk
        self.__set_chunk_st(chunk)

        # set stop time of current chunk
        self.__set_chunk_et(chunk)

        # process package
        self.__process_package(chunk, package)

        self._chunk_manager[data_type] = chunk

    def __set_chunk_st(self, chunk):
        if chunk.get_chunk_st() is None and chunk.get_last_chunk_st() is None:
            chunk.set_chunk_st(self._session_st)
        elif chunk.get_chunk_st() is None and chunk.get_last_chunk_st(
        ) is not None:
            chunk.set_chunk_st(chunk.get_last_chunk_st() + self._update_rate)

    def __set_chunk_et(self, chunk):
        if chunk.get_chunk_et() is None:
            chunk.set_chunk_et(chunk.get_chunk_st() + self._window_size)

    def __process_package(self, chunk, input_package):
        if input_package.get_timestamp() < chunk.get_chunk_st():
            # when the input data has a timestamp from the past, out-of-date data
            # print('This package is from past, do not add to current chunk')
            return
        elif input_package.get_timestamp() >= chunk.get_chunk_st(
        ) and input_package.get_timestamp() < chunk.get_chunk_et():
            # when the input data belongs to current chunk
            chunk.add_package(input_package)
        else:  # input_package.get_timestamp() >= chunk.get_chunk_et()
            # when the input data belongs to the next chunk
            copied_chunk = copy.deepcopy(chunk)

            # keep only partial of current chunk for the next turn
            keep_packages = list(
                filter(
                    lambda p: p.get_timestamp() > chunk.get_chunk_st() + self._update_rate,
                    chunk.get_packages()))

            # add current package to the chunk
            chunk.set_packages(keep_packages)
            chunk.add_package(input_package)

            # set new chunk start time and clear old ones
            chunk.set_last_chunk_st(chunk.get_chunk_st())
            chunk.set_chunk_st(None)
            chunk.set_chunk_et(None)
            chunk.increment_chunk_index()

            # send chunk to queue
            self._loop.call_soon_threadsafe(self._queue.put_nowait,
                                            copied_chunk)

    def run(self):
        self._loop.create_task(self._ws_handler())
