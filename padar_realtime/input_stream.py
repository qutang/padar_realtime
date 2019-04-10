import asyncio
import websockets
from threading import Thread
from .stream_package import SensorStreamPackage, SensorStreamChunk
import math
import copy


class InputStream(Thread):
    def __init__(self,
                 loop,
                 host,
                 port,
                 window_size,
                 update_rate,
                 session_st=None):
        Thread.__init__(self)
        self._host = host
        self._port = port
        self._loop = loop
        self._queue = asyncio.Queue(loop=self._loop)
        self._window_size = window_size
        self._update_rate = update_rate
        self._chunk_manager = {}
        self._session_st = session_st
        self._stop = False

    def get_ws_url(self):
        return 'ws://' + self._host + ":" + str(self._port)

    def change_window_size(self, window_size):
        self._window_size = window_size

    def change_update_rate(self, update_rate):
        self._update_rate = update_rate

    def set_session_st(self, session_st):
        self._session_st = session_st

    def _create_input_package(self, value):
        input_package = SensorStreamPackage()
        input_package.from_json_string(value)
        return input_package

    def stop(self):
        self._stop = True

    def send_stop_to_queue(self):
        self._loop.call_soon_threadsafe(self._queue.put_nowait, 'STOP')

    async def _ws_handler(self):
        while True:
            try:
                async with websockets.connect('ws://' + self._host + ':' +
                                              str(self._port)) as websocket:
                    try:
                        print('connected to ' + self.get_ws_url())
                        while True:
                            data = await websocket.recv()
                            if self._stop:
                                self.send_stop_to_queue()
                                break
                            self._handle_input_stream(data)
                    except Exception as e:
                        print(str(e))
                        self.send_stop_to_queue()
                        break
            except OSError as e:
                print(str(e))
                asyncio.sleep(3)
                print('Retry connect to ' + 'ws://' + self._host + ':' +
                      str(self._port))
                if self._stop:
                    self.send_stop_to_queue()
                    break
            except Exception as e:
                self.send_stop_to_queue()
                break

    async def get_chunk_stream(self):
        while True:
            chunk = await self._queue.get()
            yield chunk
            if chunk == 'STOP':
                break

    def _handle_input_stream(self, data):
        package = self._create_input_package(data)
        data_type = package.get_data_type()
        device_id = package.get_device_id()
        stream_name = package.get_stream_name()
        stream_order = package.get_stream_order()
        if data_type not in self._chunk_manager:
            chunk = SensorStreamChunk(stream_order, stream_name, device_id,
                                      data_type)
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
