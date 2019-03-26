import time
import asyncio
from pymetawear.discover import discover_devices, select_device
from pymetawear.client import MetaWearClient
import websockets
import json
import functools
from threading import Thread
import signal
import os
from .stream_package import SensorStreamPackage


class TimestampCorrector(object):
    def __init__(self, sr, method='original'):
        self._method = method
        self._last_epoch = 0
        self._current_ts = 0
        self._sr = sr
        self._interval = 1.0 / self._sr
        self._correction = 0
        self._last_real_ts = 0

    def compute_correction(self, data, real_time):
        self._correction = real_time - data['epoch'] / 1000.0

    def _next_original(self, data):
        return data['epoch'] / 1000.0 + self._correction

    def _next_noloss(self, data, last_ts):
        if last_ts == 0:
            current_ts = self._next_original(data)
        else:
            current_ts = last_ts + self._interval
        return current_ts

    def _next_withloss(self, data, last_ts, last_epoch, real_time):
        if self._last_real_ts == 0:
            self._last_real_ts = real_time
        if abs(last_epoch - data['epoch'] / 1000.0) < self._interval:
            current_ts = self._next_noloss(data, last_ts)
            real_time = self._last_real_ts + self._interval
        else:
            next_original_ts = self._next_original(data)
            next_noloss_ts = self._next_noloss(data, last_ts)
            if next_original_ts - next_noloss_ts > 2 * self._interval:
                # late for more than two intervals, renew timestamp
                current_ts = next_original_ts
            else:
                current_ts = next_noloss_ts
        self._last_epoch = self._next_original(data)
        if abs(current_ts - real_time) >= self._interval:
            current_ts = real_time
        self._last_real_ts = real_time
        return current_ts

    def next(self, data, real_time):
        if self._method == 'original':
            self._current_ts = self._next_original(data)
        elif self._method == 'noloss':
            self._current_ts = self._next_noloss(data, self._current_ts)
        elif self._method == 'withloss':
            self._current_ts = self._next_withloss(data, self._current_ts,
                                                   self._last_epoch, real_time)
        return self._current_ts


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
        self._battery_ts_corrector = TimestampCorrector(
            sr=1, method='withloss')
        self._ts_corrector_original = TimestampCorrector(
            sr=self._accel_sr, method='original')
        self._ts_corrector_noloss = TimestampCorrector(
            sr=self._accel_sr, method='noloss')
        self._ts_corrector_withloss = TimestampCorrector(
            sr=self._accel_sr, method='withloss')
        self._last_ts = 0
        self._accel_count = 0
        self._battery_count = 0

    def run(self):
        while True:
            try:
                m = MetaWearClient(
                    str(self._address), connect=True, debug=False)
            except:
                print('Retry connect to ' + self._address)
                time.sleep(1)
                continue
            break
        print("New metawear connected: {0}".format(m))
        # high frequency throughput connection setup
        m.settings.set_connection_parameters(7.5, 7.5, 0, 6000)
        # Up to 4dB for Class 2 BLE devices
        # https://github.com/hbldh/pymetawear/blob/master/pymetawear/modules/settings.py
        # https://mbientlab.com/documents/metawear/cpp/0/settings_8h.html#a335f712d5fc0587eff9671b8b105d3ed
        # Hossain AKMM, Soh WS. A comprehensive study of Bluetooth signal parameters for localization. 2007 Ieee 18th International Symposium on Personal, Indoor and Mobile Radio Communications, Vols 1-9. 2007:428-32.
        m.settings.set_tx_power(power=4)

        m.accelerometer.set_settings(
            data_rate=self._accel_sr, data_range=self._accel_grange)
        m.accelerometer.high_frequency_stream = True
        m.accelerometer.notifications(callback=self._accel_handler)
        m.settings.notifications(callback=self._battery_handler)
        self._device = m
        while True:
            time.sleep(1)
            m.settings.read_battery_state()
        return self

    def run_ws(self, loop, keep_history=True):
        self._keep_history = keep_history
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
        async for value in self.accel_stream():
            try:
                for client in self._clients:
                    await client.send(value)
            except websockets.exceptions.ConnectionClosed:
                print('ws client disconnected')
                self._clients.remove(client)
                print('remaining clients: ' + str(len(self._clients)))

    def _pack_accel_data(self, data):
        real_ts = time.time()
        if self._accel_count == 0:
            self._ts_corrector_original.compute_correction(data, real_ts)
            self._ts_corrector_noloss.compute_correction(data, real_ts)
            self._ts_corrector_withloss.compute_correction(data, real_ts)
        package = SensorStreamPackage()
        package.set_index(self._accel_count)
        package.set_device_id(self._address)
        package.set_data_type('accel')
        package.add_custom_field('HEADER_TIME_STAMP_REAL', real_ts)
        package.add_custom_field(
            'HEADER_TIME_STAMP_ORIGINAL',
            self._ts_corrector_original.next(data, real_ts))
        package.add_custom_field('HEADER_TIME_STAMP_NOLOSS',
                                 self._ts_corrector_noloss.next(data, real_ts))
        package.set_timestamp(self._ts_corrector_withloss.next(data, real_ts))
        value = {}
        value['X'] = data['value'].x
        value['Y'] = data['value'].y
        value['Z'] = data['value'].z
        package.set_value(value)

        return package.to_json_string(), package

    def _pack_battery_data(self, data):
        real_ts = time.time()
        if self._battery_count == 0:
            self._battery_ts_corrector.compute_correction(data, time.time())
        package = SensorStreamPackage()
        package.set_index(self._battery_count)
        package.set_device_id(self._address)
        package.set_data_type('battery')
        package.set_timestamp(self._battery_ts_corrector.next(data, real_ts))
        value = {}
        value['BATTERY_VOLTAGE'] = data['value'].voltage
        value['BATTERY_PERCENTAGE'] = data['value'].charge
        package.set_value(value)
        return package.to_json_string()

    def _accel_handler(self, data):
        json_package, package = self._pack_accel_data(data)
        if package._package['HEADER_TIME_STAMP_ORIGINAL'] < self._last_ts:
            print('earlier sample detected')
        else:
            self._last_ts = package._package['HEADER_TIME_STAMP_ORIGINAL']

        if self._keep_history:
            self._loop.call_soon_threadsafe(self._accel_queue.put_nowait,
                                            json_package)
        else:
            if len(self._clients) > 0:
                self._loop.call_soon_threadsafe(self._accel_queue.put_nowait,
                                                json_package)
        if self._last_minute == 0:
            print(package.to_dataframe())
            self._last_minute = time.time() * 1000.0
        if time.time() * 1000.0 - self._last_minute > 1000.0:
            print(self._address + ' sr: ' + str(self._actual_sr) + ', ' +
                  'time diff: ' +
                  str(package.get_timestamp() -
                      package._package['HEADER_TIME_STAMP_REAL']))
            self._actual_sr = 0
            self._last_minute = time.time() * 1000.0
            self._loop.call_soon_threadsafe(
                print, self._address + ' queue size: ' + str(
                    self._accel_queue.qsize()))
        else:
            self._actual_sr = self._actual_sr + 1
        self._accel_count += 1

    def _battery_handler(self, data):
        battery = data['value']
        self._loop.call_soon_threadsafe(
            print, 'battery level: ' + str(battery.voltage) + ', ' + str(
                battery.charge))
        json_package = self._pack_battery_data(data)
        if self._keep_history:
            self._loop.call_soon_threadsafe(self._accel_queue.put_nowait,
                                            json_package)
        else:
            if len(self._clients) > 0:
                self._loop.call_soon_threadsafe(self._accel_queue.put_nowait,
                                                json_package)
        self._battery_count += 1


class MetaWearStreamManager(object):
    def __init__(self, max_devices=2, init_port=8000):
        self._max_devices = max_devices
        self._init_port = init_port
        self.streams = {}

    def _reset_ble_adaptor(self):
        os.system('radiocontrol.exe Bluetooth off')
        time.sleep(1)
        os.system('radiocontrol.exe Bluetooth on')

    def _scan_for_metawears(self):
        metawears = []
        while len(metawears) < self._max_devices:
            print('scanning...')
            try:
                addr = select_device(timeout=3)
                metawears.append(addr)
            except ValueError as e:
                continue
            # metawears = list(filter(lambda d: d[1] == 'MetaWear', devices))
            time.sleep(1)
        return metawears

    def start(self,
              accel_sr=50,
              accel_grange=8,
              ws_server=True,
              keep_history=True):
        self._reset_ble_adaptor()
        metawears = self._scan_for_metawears()
        print(metawears)
        loop = asyncio.get_event_loop()

        for i in range(self._max_devices):
            address = metawears[i]
            # address = 'D5:D2:01:2B:E7:6D'
            name = 'MetaWear'
            port = self._init_port + i
            stream = MetaWearStream(
                loop,
                address,
                name,
                port=port,
                accel_sr=accel_sr,
                accel_grange=accel_grange)
            # start ws server first
            if ws_server:
                stream.run_ws(loop, keep_history=keep_history)
            try:
                stream.start()
            except KeyboardInterrupt:
                stream.stop()
            # rest for a second before connecting to the next stream
            time.sleep(1)

        if ws_server:
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            loop.run_forever()
