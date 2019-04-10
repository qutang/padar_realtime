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
from mbientlab.metawear import libmetawear
from mbientlab.metawear import cbindings
import logging


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
                 order,
                 host='localhost',
                 port=8000,
                 accel_sr=50,
                 accel_grange=8):
        Thread.__init__(self)
        self._device = None
        self._model_name = 'NA'
        self._accel_sr = accel_sr
        self._accel_grange = accel_grange
        self._address = address
        self._name = name
        self._order = order
        self._clients = set()
        self._host = host
        self._port = port
        self._actual_sr = 0
        self._last_minute = 0
        if loop is not None:
            self.set_loop(loop)
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
        self._stop = False
        self._status = "Available"

    def set_loop(self, loop):
        self._loop = loop
        self._accel_queue = asyncio.Queue(loop=self._loop)
        self._start_ws_server = websockets.serve(
            self._ws_handler,
            host=self._host,
            port=self._port,
            loop=self._loop)

    def get_device_address(self):
        return self._address

    def get_stream_name(self):
        return self._name

    def get_ws_uri(self):
        return 'ws://' + self._host + ':' + str(self._port)

    def run(self):
        while not self._stop:
            try:
                m = MetaWearClient(
                    str(self._address), connect=True, debug=False)
                self._device = m
                self._model_name = self.get_model_name()
            except:
                logging.info('Retry connect to ' + self._address)
                time.sleep(1)
                continue
            break
        logging.info("New metawear connected: {0}".format(m))
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
        self._status = "Stream"
        while not self._stop:
            time.sleep(1)
            m.settings.read_battery_state()
        return self

    def get_model_name(self):
        model_code = libmetawear.mbl_mw_metawearboard_get_model(
            self._device.mw.board)
        metawear_models = cbindings.Model()
        model_names = list(
            filter(lambda attr: '__' not in attr, dir(metawear_models)))
        for name in model_names:
            if getattr(metawear_models, name) == model_code:
                return name
        return 'NA'

    def get_status(self):
        return self._status

    def run_ws(self, keep_history=True):
        self._keep_history = keep_history
        logging.info('Start ws server on: ' + self.get_ws_uri())
        self._server = self._loop.run_until_complete(self._start_ws_server)

    def stop(self):
        logging.info('Disconnect from sensor')
        success = False
        try:
            self._device.accelerometer.stop()
            self._device.disconnect()
        except Exception as e:
            logging.error(str(e))
        logging.info('Stop ws server')
        try:
            self._stop = True
            self._server.close()
            self._loop.run_until_complete(self._server.wait_closed())
            self._status = "Available"
            success = True
        except Exception as e:
            logging.error(str(e))
        return success

    async def accel_stream(self):
        while True:
            value = await self._accel_queue.get()
            yield value

    async def _ws_handler(self, client, path):
        logging.info('ws client is added')
        self._clients.add(client)
        async for value in self.accel_stream():
            try:
                for client in self._clients:
                    await client.send(value)
                if self._stop:
                    break
            except websockets.exceptions.ConnectionClosed:
                logging.info('ws client disconnected')
                self._clients.remove(client)
                logging.info('remaining clients: ' + str(len(self._clients)))

    def _calibrate_accel_coord_system(self, x, y, z):
        # axis values are calibrated according to the coordinate system of Actigraph GT9X
        # http://www.correctline.pl/wp-content/uploads/2015/01/ActiGraph_Device_Axes_Link.png
        if self._model_name == 'METAMOTION_R':
            # as normal wear in the case on wrist
            calibrated_x = y
            calibrated_y = -x
            calibrated_z = z
        else:
            calibrated_x = x
            calibrated_y = y
            calibrated_z = z
        return (calibrated_x, calibrated_y, calibrated_z)

    def _pack_accel_data(self, data):
        real_ts = time.time()
        if self._accel_count == 0:
            self._ts_corrector_original.compute_correction(data, real_ts)
            self._ts_corrector_noloss.compute_correction(data, real_ts)
            self._ts_corrector_withloss.compute_correction(data, real_ts)
        package = SensorStreamPackage()
        package.set_index(self._accel_count)
        package.set_device_id(self._address)
        package.set_stream_name(self._name)
        package.set_stream_order(self._order)
        package.set_data_type('accel')
        package.add_custom_field('HEADER_TIME_STAMP_REAL', real_ts)
        package.add_custom_field(
            'HEADER_TIME_STAMP_ORIGINAL',
            self._ts_corrector_original.next(data, real_ts))
        package.add_custom_field('HEADER_TIME_STAMP_NOLOSS',
                                 self._ts_corrector_noloss.next(data, real_ts))
        package.set_timestamp(self._ts_corrector_withloss.next(data, real_ts))
        value = {}
        calibrated_values = self._calibrate_accel_coord_system(
            data['value'].x, data['value'].y, data['value'].z)

        value['X'] = calibrated_values[0]
        value['Y'] = calibrated_values[1]
        value['Z'] = calibrated_values[2]
        package.set_value(value)

        return package.to_json_string(), package

    def _pack_battery_data(self, data):
        real_ts = time.time()
        if self._battery_count == 0:
            self._battery_ts_corrector.compute_correction(data, time.time())
        package = SensorStreamPackage()
        package.set_index(self._battery_count)
        package.set_device_id(self._address)
        package.set_stream_name(self._name)
        package.set_stream_order(self._order)
        package.set_data_type('battery')
        package.set_timestamp(self._battery_ts_corrector.next(data, real_ts))
        value = {}
        value['BATTERY_VOLTAGE'] = data['value'].voltage
        value['BATTERY_PERCENTAGE'] = data['value'].charge
        package.set_value(value)
        return package.to_json_string()

    def _accel_handler(self, data):
        self._status = "Stream"
        json_package, package = self._pack_accel_data(data)
        if package._package['HEADER_TIME_STAMP_ORIGINAL'] < self._last_ts:
            logging.warning('earlier sample detected')
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
            logging.debug(package.to_dataframe())
            self._last_minute = time.time() * 1000.0
        if time.time() * 1000.0 - self._last_minute > 1000.0:
            logging.info(self._address + ' sr: ' + str(self._actual_sr) +
                         ', ' + 'time diff: ' +
                         str(package.get_timestamp() -
                             package._package['HEADER_TIME_STAMP_REAL']))
            self._actual_sr = 0
            self._last_minute = time.time() * 1000.0
            self._loop.call_soon_threadsafe(
                logging.debug, self._address + ' queue size: ' + str(
                    self._accel_queue.qsize()))
        else:
            self._actual_sr = self._actual_sr + 1
        self._accel_count += 1

    def _battery_handler(self, data):
        battery = data['value']
        self._loop.call_soon_threadsafe(
            logging.info, 'battery level: ' + str(battery.voltage) + ', ' +
            str(battery.charge))
        json_package = self._pack_battery_data(data)
        if self._keep_history:
            self._loop.call_soon_threadsafe(self._accel_queue.put_nowait,
                                            json_package)
        else:
            if len(self._clients) > 0:
                self._loop.call_soon_threadsafe(self._accel_queue.put_nowait,
                                                json_package)
        self._battery_count += 1


class MetaWearScanner():
    def scan(self):
        metawears = set()
        retries = 0
        while retries < 3:
            logging.info('Scanning metawear devices nearby...')
            try:
                retries += 1
                candidates = discover_devices(timeout=5)
                metawears |= set(
                    map(lambda d: d[0],
                        filter(lambda d: d[1] == 'MetaWear', candidates)))
            except ValueError as e:
                logging.error(str(e))
                continue
        return list(metawears)


class MetaWearStreamManager(object):
    def __init__(self, max_devices=2, init_port=8000):
        self._max_devices = max_devices
        self._init_port = init_port
        self.streams = []
        self._loop = asyncio.new_event_loop()

    def reset_ble_adaptor(self):
        os.system('radiocontrol.exe Bluetooth off')
        time.sleep(1)
        os.system('radiocontrol.exe Bluetooth on')

    def _scan_for_metawears(self):
        metawears = []
        metawear_stream_names = []
        while len(metawears) < self._max_devices:
            logging.info('Scanning metawear devices nearby...')
            try:
                addr = select_device(timeout=3)
                stream_name = input('enter stream name: ')
                metawears.append(addr)
                metawear_stream_names.append(stream_name)
            except ValueError as e:
                continue
            # metawears = list(filter(lambda d: d[1] == 'MetaWear', devices))
            time.sleep(1)
        return metawears, metawear_stream_names

    def _autoscan_for_metawears(self):
        metawears = set()
        retries = 0
        while len(metawears) < self._max_devices and retries <= 5:
            logging.info('Scanning metawear devices nearby...')
            try:
                retries += 1
                candidates = discover_devices(timeout=3)
                metawears |= set(
                    map(lambda d: d[0],
                        filter(lambda d: d[1] == 'MetaWear', candidates)))
            except ValueError as e:
                continue
        return metawears

    def get_status(self, addr):
        for stream in self.streams:
            if stream.get_device_address() == addr:
                return stream.get_status()
        return "Unknown"

    def get_stream_name(self, addr):
        for stream in self.streams:
            if stream.get_device_address() == addr:
                return stream.get_stream_name()
        return None

    def get_sr(self, addr):
        for stream in self.streams:
            if stream.get_device_address() == addr:
                return stream._accel_sr
        return None

    def get_grange(self, addr):
        for stream in self.streams:
            if stream.get_device_address() == addr:
                return stream._accel_grange
        return None

    def get_order(self, addr):
        for stream in self.streams:
            if stream.get_device_address() == addr:
                return stream._order
        return None

    def get_ws_url(self, addr):
        for stream in self.streams:
            if stream.get_device_address() == addr:
                return stream.get_ws_uri()
        return None

    def disconnect(self, addr):
        to_remove = None
        success = False
        for stream in self.streams:
            if stream.get_device_address() == addr:
                success = stream.stop()
                to_remove = stream
                break
        if to_remove is not None:
            if success:
                self.streams.remove(to_remove)
                return "Success"
            else:
                return "Failure"
        else:
            logging.info("Can not find the stream with addr: " + addr)
            return "Not found"

    def scan(self):
        metawears = self._autoscan_for_metawears()
        logging.debug(metawears)
        return metawears

    def stream_exists(self, stream):
        for s in self.streams:
            if stream.get_device_address() == s.get_device_address():
                return 'Address in use'
            elif stream.get_stream_name() == s.get_stream_name():
                return 'Name in use'
            elif stream._order == s._order:
                return 'Order in use'
        return False

    def init(self, metawears, loop):
        self._loop = loop
        results = metawears
        for metawear in metawears:
            i = metawears.index(metawear)
            addr = metawear['address']
            name = metawear['name']
            order = metawear['order']
            accel_sr = metawear['sr']
            accel_grange = metawear['grange']
            port = self._init_port + order
            stream = MetaWearStream(
                self._loop,
                addr,
                name,
                order,
                port=port,
                accel_sr=accel_sr,
                accel_grange=accel_grange)
            error_code = self.stream_exists(stream)
            logging.debug(error_code)
            if error_code == False:
                logging.info('Start to run a sensor')
                self.streams.append(stream)
                stream.run_ws(keep_history=True)
                try:
                    stream.start()
                    results[i]['ws_uri'] = stream.get_ws_uri()
                except KeyboardInterrupt:
                    stream.stop()
                    results[i]['ws_uri'] = 'failed to start'
                # rest for a second before connecting to the next stream
                time.sleep(1)
            else:
                logging.warning('error in running sensor')
                results[i]['error_code'] = error_code
        return results

    def start(self,
              accel_sr=50,
              accel_grange=8,
              ws_server=True,
              keep_history=True):
        self.reset_ble_adaptor()
        metawears, metawear_stream_names = self._scan_for_metawears()
        logging.debug(metawears)
        logging.debug(metawear_stream_names)

        for i in range(self._max_devices):
            address = metawears[i]
            # address = 'D5:D2:01:2B:E7:6D'
            name = metawear_stream_names[i]
            port = self._init_port + i
            stream = MetaWearStream(
                self._loop,
                address,
                name,
                i,
                port=port,
                accel_sr=accel_sr,
                accel_grange=accel_grange)
            self.streams.append(stream)
            # start ws server first
            if ws_server:
                stream.run_ws(keep_history=keep_history)
            try:
                stream.start()
            except KeyboardInterrupt:
                stream.stop()
            # rest for a second before connecting to the next stream
            time.sleep(1)

        if ws_server:
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            self._loop.run_forever()
