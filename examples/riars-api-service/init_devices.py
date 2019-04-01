from padar_realtime.metawear_stream import MetaWearStreamManager
from flask import jsonify


def init_devices_v1(metawears, loop):
    manager = MetaWearStreamManager(max_devices=2, init_port=8000)
    results = manager.init(metawears, loop)
    return jsonify(results)