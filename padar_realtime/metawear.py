import time
import asyncio
from pymetawear.discover import discover_devices
from pymetawear.client import MetaWearClient
import websockets
import json
import functools
from threading import Thread
import signal
import os


class MetaWearStream(Thread):
    def __init__(self,
                 loop,
                 address,
                 name,
                 host='localhost',
                 port=8000,
                 accel_sr=50,
                 accel_grange=8):
        Thread.__init__(self)
        self._device = None
        self._accel_sr = accel_sr
        self._accel_grange = accel_grange
        self._address = address
        self._name = name
        self._accel_queue = asyncio.Queue()
        self._clients = set()
        self._host = host
        self._port = port
        self._server = websockets.serve(self._ws_handler, host=host, port=port)
        self._actual_sr = 0
        self._last_minute = 0
        self._loop = loop
        self._current_ts = 0

    def run(self):
        while True:
            try:
                m = MetaWearClient(
                    str(self._address), connect=True, debug=False)
            except:
                print('Retry connect to ' + self._address)
                time.sleep(2)
                continue
            break
        print("New metawear connected: {0}".format(m))
        m.accelerometer.set_settings(
            data_rate=self._accel_sr, data_range=self._accel_grange)
        m.accelerometer.high_frequency_stream = False
        m.accelerometer.notifications(callback=self._handler)
        self._device = m
        while True:
            time.sleep(1)
        return self

    def run_ws(self, loop):
        print('Start ws server on: ' + self._host + ':' + str(self._port))
        loop.run_until_complete(self._server)

    def stop(self):
        print('Disconnect from sensor')
        self._device.disconnect()

    async def accel_stream(self):
        while True:
            value = await self._accel_queue.get()
            yield value

    async def _ws_handler(self, client, path):
        print('ws client is added')
        self._clients.add(client)
        try:
            async for value in self.accel_stream():
                for client in self._clients:
                    await client.send(value)
        except websockets.exceptions.ConnectionClosed:
            print('ws client disconnected')
            self._clients.remove(client)
        finally:
            print('remaining clients: ' + str(len(self._clients)))

    def _pack_accel_data(self, data):
        result = {}
        result['ID'] = self._address
        if self._current_ts == 0:
            self._current_ts = data['epoch'] / 1000.0
        else:
            self._current_ts = self._current_ts + (1.0 / float(self._accel_sr))
        result['HEADER_TIME_STAMP'] = self._current_ts
        result['X'] = data['value'].x
        result['Y'] = data['value'].y
        result['Z'] = data['value'].z
        return json.dumps(result)

    def _handler(self, data):
        if self._last_minute == 0:
            self._last_minute = time.time() * 1000.0
        if time.time() * 1000.0 - self._last_minute > 1000.0:
            print(self._address + ' sr: ' + str(self._actual_sr))
            self._actual_sr = 0
            self._last_minute = time.time() * 1000.0
            self._loop.call_soon_threadsafe(
                print, self._address + ' queue size: ' + str(
                    self._accel_queue.qsize()))
        else:
            self._actual_sr = self._actual_sr + 1
        result = self._pack_accel_data(data)
        if len(self._clients) > 0:
            self._loop.call_soon_threadsafe(self._accel_queue.put_nowait,
                                            result)


class MetaWearStreamManager(object):
    def __init__(self, max_devices=1, init_port=8000):
        self._max_devices = max_devices
        self._init_port = init_port
        self.streams = {}

    def start(self, accel_sr=50, accel_grange=8, ws_server=True):
        os.system('radiocontrol.exe Bluetooth off')
        time.sleep(1)
        os.system('radiocontrol.exe Bluetooth on')
        metawears = []
        while len(metawears) < self._max_devices:
            print('scanning...')
            devices = discover_devices(timeout=3)
            metawears = list(filter(lambda d: d[1] == 'MetaWear', devices))
            time.sleep(1)
        print(metawears)
        loop = asyncio.get_event_loop()
        for i in range(self._max_devices):
            address = metawears[i][0]
            name = metawears[i][1]
            port = self._init_port + i
            stream = MetaWearStream(
                loop,
                address,
                name,
                port=port,
                accel_sr=accel_sr,
                accel_grange=accel_grange)
            try:
                stream.start()
            except KeyboardInterrupt:
                stream.stop()
            if ws_server:
                stream.run_ws(loop)
            time.sleep(2)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        if ws_server:
            loop.run_forever()


if __name__ == '__main__':
    stream_manager = MetaWearStreamManager(max_devices=2)
    stream_manager.start(ws_server=True)
