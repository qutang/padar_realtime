import sys
from padar_realtime.metawear_stream import MetaWearStreamManager

if __name__ == '__main__':
    num_of_devices = int(sys.argv[1])
    stream_manager = MetaWearStreamManager(max_devices=num_of_devices)
    stream_manager.start(ws_server=True, accel_sr=50, keep_history=True)