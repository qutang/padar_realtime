"""
flask server using chart.js and websocket to show real-time graphing

Author: Qu Tang

Date: Jul 06, 2018
"""
from flask import Flask
from flask import render_template
import arrow
from dateutil import tz
import numpy as np
from random import random
import sys


class VisualizationApp(object):
    def __init__(self, num_of_devices=2, num_of_tasks=1):
        self._app = Flask(__name__)
        self._app_config = {
            'max_sensors':
            num_of_devices,
            'num_of_tasks':
            num_of_tasks,
            'sensor_chart_ids':
            ['sensor_ts_chart_' + str(i) for i in range(0, num_of_devices)],
            'feature_chart_ids':
            ['feature_ts_chart_' + str(i) for i in range(0, num_of_devices)],
            'prediction_chart_ids':
            ['prediction_ts_chart_' + str(i) for i in range(0, num_of_tasks)],
            'sensor_ports': [8000 + i for i in range(0, num_of_devices)],
            'ar_port':
            9000,
            'url':
            'localhost',
            'refresh_rate':
            0.1,
            'window_size':
            12.8
        }
        self._app.add_url_rule('/', 'App', self.realtime_ar)

    def realtime_ar(self):
        return render_template(
            "layouts/realtime_ar.j2",
            title='Real-time AR performance analysis',
            config=self._app_config)

    def start(self, debug=False):
        self._app.run(debug=debug)


if __name__ == '__main__':
    num_of_devices = int(sys.argv[1])
    num_of_tasks = int(sys.argv[2])
    app = VisualizationApp(
        num_of_devices=num_of_devices, num_of_tasks=num_of_tasks)
    app.start()
