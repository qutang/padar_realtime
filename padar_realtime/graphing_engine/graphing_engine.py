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

app = Flask(__name__)

app_config = {
    'sensor_socket': {
        'url': 'localhost',
        'port': 8848,
        'rate': 0.5
    }
}


@app.route("/default_chart")
def default_chart():
    legend = 'Monthly Data'
    labels = ["January", "February", "March",
              "April", "May", "June", "July", "August"]
    values = [10, 9, 8, 7, 6, 4, 7, 8]
    return render_template('default_chart.j2',
                           values=values, labels=labels, legend=legend)


@app.route("/")
def realtime_ar():
    legend = 'Temperatures'
    length = 50 * 60
    st = arrow.utcnow().to(tz.tzlocal()).float_timestamp
    times = np.arange(st, st + 0.02 * length, step=0.02)
    times = list(map(lambda t: arrow.Arrow.fromtimestamp(
        t).format('YYYY-MM-DD HH:mm:ss.SSS'), times))
    data = {
        'x': [],
        'y': [],
        'z': []
    }
    for i in range(0, length):
        data['x'].append({
            'y': random() + 1,
            'x': times[i]
        })
        data['y'].append({
            'y': random() - 1,
            'x': times[i]
        })
        data['z'].append({
            'y': random(),
            'x': times[i]
        })

    return render_template("layouts/realtime_ar.j2",
                           title='Real-time AR performance analysis',
                           values=data, legend=legend, config=app_config)


def start(debug=False):
    app.run(debug=debug)


if __name__ == '__main__':
    start()
