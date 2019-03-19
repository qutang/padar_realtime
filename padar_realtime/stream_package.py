from enum import Enum
import json
import pandas as pd


class SensorStreamChunk(object):
    def __init__(self, stream_name, device_id, data_type):
        self._chunk = {
            'LAST_CHUNK': None,
            'START_TIME': None,
            'STOP_TIME': None,
            'STREAM_NAME': stream_name,
            'DEVICE_ID': device_id,
            'PACKAGES': [],
            'DATA_TYPE': data_type,
            'CHUNK_INDEX': 0
        }

    def add_package(self, package):
        self._chunk['PACKAGES'].append(package)

    def set_packages(self, packages):
        self._chunk['PACKAGES'] = packages

    def get_packages(self):
        return self._chunk['PACKAGES']

    def get_data_type(self):
        return self._chunk['DATA_TYPE']

    def get_stream_name(self):
        return self._chunk['STREAM_NAME']

    def get_device_id(self):
        return self._chunk['DEVICE_ID']

    def get_chunk_index(self):
        return self._chunk['CHUNK_INDEX']

    def set_data_type(self, data_type):
        self._chunk['DATA_TYPE'] = data_type

    def set_last_chunk(self, last_chunk):
        self._chunk['LAST_CHUNK'] = last_chunk
        self._chunk['CHUNK_INDEX'] = last_chunk.get_chunk_index() + 1

    def clear_last_chunk(self):
        self._chunk['LAST_CHUNK'] = None

    def get_last_chunk(self):
        return self._chunk['LAST_CHUNK']

    def set_chunk_st(self, chunk_st):
        self._chunk['START_TIME'] = chunk_st

    def set_chunk_et(self, chunk_et):
        self._chunk['STOP_TIME'] = chunk_et

    def get_chunk_st(self):
        return self._chunk['START_TIME']

    def get_chunk_et(self):
        return self._chunk['STOP_TIME']

    def to_json_string(self):
        return json.dumps(self._chunk)

    def from_json_string(self, json_chunk):
        self._chunk = json.loads(json_chunk)


class SensorStreamPackage(object):
    def __init__(self):
        self._package = {
            'HEADER_TIME_STAMP': None,
            'DATA_TYPE': None,
            'DEVICE_ID': None,
            'STREAM_NAME': None,
            'VALUE': {},
            'PACKAGE_INDEX': 0
        }

    def set_index(self, index):
        self._package['PACKAGE_INDEX'] = index

    def get_value_entries(self):
        return self._package['VALUE'].keys()

    def get_data_type(self):
        return self._package['DATA_TYPE']

    def get_timestamp(self):
        return self._package['HEADER_TIME_STAMP']

    def get_device_id(self):
        return self._package['DEVICE_ID']

    def set_timestamp(self, timestamp):
        self._package['HEADER_TIME_STAMP'] = timestamp

    def set_value(self, value):
        self._package['VALUE'] = value

    def set_device_id(self, device_id):
        self._package['DEVICE_ID'] = device_id

    def set_stream_name(self, stream_name):
        self._package['STREAM_NAME'] = stream_name

    def get_stream_name(self):
        return self._package['STREAM_NAME']

    def set_data_type(self, data_type):
        self._package['DATA_TYPE'] = data_type

    def add_custom_field(self, name, value):
        self._package[name] = value

    def to_json_string(self):
        return json.dumps(self._package)

    def from_json_string(self, json_package):
        self._package = json.loads(json_package)

    def to_dataframe(self):
        df_dict = {}
        value_dict = None
        for name, value in self._package.items():
            if name == 'VALUE':
                value_dict = value
            else:
                df_dict[name] = value
        df_dict = {**df_dict, **value_dict}
        return pd.DataFrame.from_records([df_dict])
