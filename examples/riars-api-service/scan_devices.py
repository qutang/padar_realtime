from padar_realtime.metawear_stream import MetaWearStreamManager
from flask import jsonify


def scan_devices_v1(num_of_devices=1):
    manager = MetaWearStreamManager(max_devices=num_of_devices, init_port=8000)
    metawears = manager.scan()
    return jsonify(list(metawears))
