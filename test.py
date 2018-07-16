import logging
import sys
from padar_realtime.sensor_engine import SensorEngine

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
SensorEngine(port=2222).produce_accelerometer(6, 50)