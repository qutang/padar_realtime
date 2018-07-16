"""

Real-time engine to generate various sensor data

Author: Qu Tang

Date: Jul 05, 2018

"""
import asyncio
from random import random
import time
from .libs.websocket_server import WebsocketServer
import logging

logger = logging.getLogger()


class SensorEngine:
    def __init__(self, port=20001):
        self._server = WebsocketServer(port=port)
        self._real_sr = 0
        self._last_ts = 0

    def produce_accelerometer(self, grange, sr, socket='websocket', activity='random'):
        if activity == 'random':
            data_func = lambda: random() * grange * 2 - grange
        else:
            raise NotImplementedError('Unrecognized activity argument' + activity)
        if socket == 'websocket':
            async def _producer():
                await asyncio.sleep(1.0 / float(sr))
                ts = time.time()
                data = {
                    'HEADER_TIME_STAMP': ts,
                    'X': data_func(),
                    'Y': data_func(),
                    'Z': data_func()
                }
                self._real_sr = self._real_sr + 1
                if self._last_ts == 0:
                    self._last_ts = ts
                elif ts - self._last_ts >= 10:
                    logger.info('Real sampling rate: ' + str(self._real_sr) + " Hz")
                    self._real_sr = 0
                    self._last_ts = ts
                return data
            self._server.make_producer({
                'func': _producer,
                'desc': 'Producer for ' + activity + ' accelerometer signal at ' + str(sr) + ' Hz and ' + str(grange) + ' G range.',
                'kwargs': {}
            }).start()
        else:
            raise NotImplementedError('Unrecognized socket argument:' + socket)
