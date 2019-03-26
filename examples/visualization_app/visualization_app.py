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
    def __init__(self, num_of_devices=2):
        self._app = Flask(__name__)
        self._app_config = {
            'max_sensors': num_of_devices,
            'init_port': 8000,
            'init_ar_port': 9000,
            'url': 'localhost',
            'refresh_rate': 0.1
        }
        self._app.add_url_rule('/default_chart', 'default chart',
                               self.default_chart)
        self._app.add_url_rule('/', 'App', self.realtime_ar)

    def default_chart(self):
        legend = 'Monthly Data'
        labels = [
            "January", "February", "March", "April", "May", "June", "July",
            "August"
        ]
        values = [10, 9, 8, 7, 6, 4, 7, 8]
        return render_template(
            'default_chart.j2', values=values, labels=labels, legend=legend)

    def realtime_ar(self):
        legend = 'Acceleration'
        length = 50 * 60
        st = arrow.utcnow().to(tz.tzlocal()).float_timestamp
        times = np.arange(st, st + 0.02 * length, step=0.02)
        times = list(
            map(
                lambda t: arrow.Arrow.fromtimestamp(t).format('YYYY-MM-DD HH:mm:ss.SSS'),
                times))
        data = {'x': [], 'y': [], 'z': []}
        for i in range(0, length):
            data['x'].append({'y': random() + 1, 'x': times[i]})
            data['y'].append({'y': random() - 1, 'x': times[i]})
            data['z'].append({'y': random(), 'x': times[i]})

        return render_template(
            "layouts/realtime_ar.j2",
            title='Real-time AR performance analysis',
            values=data,
            legend=legend,
            config=self._app_config)

    def start(self, debug=False):
        self._app.run(debug=debug)


if __name__ == '__main__':
    num_of_devices = int(sys.argv[1])
    app = VisualizationApp(num_of_devices=num_of_devices)
    app.start()
