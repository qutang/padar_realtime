function SensorChartEngine(chart_id, url, port, refresh_rate) {
    this._chart_colors = {
        red: 'rgb(255, 99, 132)',
        orange: 'rgb(255, 159, 64)',
        yellow: 'rgb(255, 205, 86)',
        green: 'rgb(75, 192, 192)',
        blue: 'rgb(54, 162, 235)',
        purple: 'rgb(153, 102, 255)',
        grey: 'rgb(201, 203, 207)'
    };
    this._url = url;
    this._port = port;
    this._refresh_rate = refresh_rate;
    this._chart_id = chart_id;
    this._chart_index = parseInt(this._chart_id.split('_').pop());
    this._chart_ctx = document.getElementById(this._chart_id).getContext("2d");
    this._worker = new Worker('static/webworker/data_receiver.js');
    this._initData();
    this._initChart();
    this._initEvents();
}

SensorChartEngine.prototype._initData = function () {
    this._chart_data = {
        datasets: [{
            label: 'x',
            fill: false,
            lineTension: 0.2,
            borderColor: this._chart_colors.blue,
            borderCapStyle: 'butt',
            borderDash: [],
            borderDashOffset: 0.0,
            borderJoinStyle: 'miter',
            pointBorderColor: this._chart_colors.blue,
            pointBackgroundColor: "#fff",
            pointBorderWidth: 1,
            pointHoverRadius: 5,
            pointHoverBackgroundColor: this._chart_colors.blue,
            pointHoverBorderColor: this._chart_colors.blue,
            pointHoverBorderWidth: 2,
            pointRadius: 1,
            pointHitRadius: 10,
            data: [],
            spanGaps: false
        }, {
            label: 'y',
            fill: false,
            lineTension: 0.2,
            borderColor: this._chart_colors.red,
            borderCapStyle: 'butt',
            borderDash: [],
            borderDashOffset: 0.0,
            borderJoinStyle: 'miter',
            pointBorderColor: this._chart_colors.red,
            pointBackgroundColor: "#fff",
            pointBorderWidth: 1,
            pointHoverRadius: 5,
            pointHoverBackgroundColor: this._chart_colors.red,
            pointHoverBorderColor: this._chart_colors.red,
            pointHoverBorderWidth: 2,
            pointRadius: 1,
            pointHitRadius: 10,
            data: [],
            spanGaps: false
        }, {
            label: 'z',
            fill: false,
            lineTension: 0.2,
            borderColor: this._chart_colors.green,
            borderCapStyle: 'butt',
            borderDash: [],
            borderDashOffset: 0.0,
            borderJoinStyle: 'miter',
            pointBorderColor: this._chart_colors.green,
            pointBackgroundColor: "#fff",
            pointBorderWidth: 1,
            pointHoverRadius: 5,
            pointHoverBackgroundColor: this._chart_colors.green,
            pointHoverBorderColor: this._chart_colors.green,
            pointHoverBorderWidth: 2,
            pointRadius: 1,
            pointHitRadius: 10,
            data: [],
            spanGaps: false
        }, {
            label: 'battery',
            fill: false,
            lineTension: 0.2,
            borderColor: this._chart_colors.grey,
            borderCapStyle: 'butt',
            borderDash: [],
            borderDashOffset: 0.0,
            borderJoinStyle: 'miter',
            pointBorderColor: this._chart_colors.grey,
            pointBackgroundColor: "#fff",
            pointBorderWidth: 1,
            pointHoverRadius: 5,
            pointHoverBackgroundColor: this._chart_colors.grey,
            pointHoverBorderColor: this._chart_colors.grey,
            pointHoverBorderWidth: 2,
            pointRadius: 1,
            pointHitRadius: 10,
            data: [],
            spanGaps: false
        }]
    }
}

SensorChartEngine.prototype._initChart = function () {
    this._chart = new Chart(this._chart_ctx, {
        type: 'line',
        data: this._chart_data,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            downsample: {
                enabled: true,
                threshold: 500, // change this
                auto: false, // don't re-downsample the data every move
                onInit: true, // but do resample it when we init the chart (this is default)
                preferOriginalData: true, // use our original data when downscaling so we can downscale less, if we need to.
                restoreOriginalData: false, // if auto is false and this is true, original data will be restored on pan/zoom - that isn't what we want.
            },
            zoom: {
                // Boolean to enable zooming
                enabled: false,
                // Enable drag-to-zoom behavior
                drag: true,
                // Zooming directions. Remove the appropriate direction to disable 
                // Eg. 'y' would only allow zooming in the y direction
                mode: 'x',
                rangeMin: {
                    // Format of min zoom range depends on scale type
                    x: null,
                    y: null
                },
                rangeMax: {
                    // Format of max zoom range depends on scale type
                    x: null,
                    y: null
                }
            },
            //showLines: false, // disable for all datasets
            scales: {
                xAxes: [{
                    type: 'time',
                    time: {
                        unit: 'millisecond',
                        stepSize: 5000,
                        displayFormats: {
                            millisecond: 'HH:mm:ss.SSS'
                        }
                    }
                }]
            }
        }
    });
}

SensorChartEngine.prototype._initEvents = function () {
    // register reset zoom event
    var engine = this;
    $('#' + engine._chart_id + '-reset-zoom').click(function () {
        engine._chart.resetZoom();
    });
    $('#' + engine._chart_id + '-connect').click(function () {
        if ($(this).text() === 'Connect') {
            engine.connect();
            $(this).text('Disconnect');
        } else if ($(this).text() === 'Disconnect') {
            engine.disconnect();
            $(this).text('Connect');
        }
    });
}

SensorChartEngine.prototype._addChartData = function (data) {
    var duration = 10; // seconds
    var common_start_ts = 0
    var common_end_ts = 0
    this._chart.data.datasets.forEach((dataset) => {
        var start_ts = 0
        var end_ts = 0
        if (dataset.label !== 'battery') {
            var n_new = data[dataset.label].length;
            if (n_new > 0) {
                end_ts = data[dataset.label][n_new - 1]['x'].valueOf() / 1000.0;
            }
            var start_ts = end_ts
            if (dataset.data.length != 0) {
                start_ts = dataset.data[0]['x'].valueOf() / 1000.0;
            }
            common_start_ts = start_ts
            common_end_ts = end_ts
        } else {
            start_ts = common_start_ts
            end_ts = common_end_ts
        }
        if (end_ts - start_ts > duration) { // if there are more than 10s data
            keep_ts_start = end_ts - duration
            dataset.data = dataset.data.filter(function (s) { return s['x'].valueOf() / 1000.0 >= keep_ts_start })
            console.log('dataset length (after filter) ' + dataset.label + ': ' + dataset.data.length);
        }
        dataset.data = dataset.data.concat(data[dataset.label]);
        console.log('dataset length ' + dataset.label + ': ' + dataset.data.length);
    });
    this._chart.update({
        duration: 0
    });
}

SensorChartEngine.prototype.updateChartData = function (data) {
    var converted_data = this._convertData(data);
    this._addChartData(converted_data);
}

SensorChartEngine.prototype._convertData = function (stream) {
    sensor_stream = stream.filter(function (sample) {
        return 'accel' === sample['DATA_TYPE']
    });
    battery_stream = stream.filter(function (sample) {
        return 'battery' === sample['DATA_TYPE']
    });
    var converted_data = {
        'x': [],
        'y': [],
        'z': [],
        'battery': []
    };
    converted_data['x'] = sensor_stream.map(function (sample) { return { x: moment.unix(sample['HEADER_TIME_STAMP']), y: sample['VALUE']['X'] } });
    converted_data['y'] = sensor_stream.map(function (sample) { return { x: moment.unix(sample['HEADER_TIME_STAMP']), y: sample['VALUE']['Y'] } });
    converted_data['z'] = sensor_stream.map(function (sample) { return { x: moment.unix(sample['HEADER_TIME_STAMP']), y: sample['VALUE']['Z'] } });
    var all_values = converted_data['x'].map(function (x) { return x['y'] }).concat(converted_data['y'].map(function (x) { return x['y'] })).concat(converted_data['z'].map(function (x) { return x['y'] }))

    var current_max = 0;
    if (battery_stream.length > 0) {
        current_max = ss.max(all_values)
    }

    if (battery_stream.length > 0) {
        converted_data['battery'] = battery_stream.map(function (sample) { return { x: moment.unix(sample['HEADER_TIME_STAMP']), y: sample['VALUE']['BATTERY_PERCENTAGE'] / 100.0 } })
    }
    return converted_data;
}

SensorChartEngine.prototype.connect = function () {
    var engine = this;
    // register callback when receiving data from worker
    this._worker.onmessage = function (e) {
        if (e.data['action'] == 'error') {
            engine._worker.terminate();
            engine.disconnect();
        } else if (e.data['action'] == 'data') {
            if (e.data.content && e.data.content.length > 0) {
                // console.log('Receiving data buffer of size: ' + e.data.content.length);
                engine.updateChartData(e.data.content);
            }
        }
    };

    this._worker.postMessage({
        'action': 'start',
        'url': engine._url,
        'port': engine._port,
        'rate': engine._refresh_rate
    })
    console.log('Connecting to ws://' + engine._url + ':' + engine._port + ' at refresh rate: ' + engine._refresh_rate + ' seconds...');
}

SensorChartEngine.prototype.disconnect = function () {
    this._worker.postMessage({
        'action': 'stop',
        'port': this._port
    });
}