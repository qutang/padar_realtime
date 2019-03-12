function FeatureChartEngine(chart_id, url, port, refresh_rate) {
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
    this._chart_duration = 10;
    this._chart_index = parseInt(this._chart_id.split('_').pop());
    this._chart_ctx = document.getElementById(this._chart_id).getContext("2d");
    this._worker = new Worker('static/webworker/data_receiver.js');
    this._initEvents();
}

FeatureChartEngine.prototype._initData = function (data) {
    var names = Object.keys(data);
    var color_names = Object.keys(this._chart_colors);
    var datasets = names.map(function (name, index) {
        var color = this._chart_colors[color_names[index % color_names.length]];
        var dataset = {
            label: name,
            fill: false,
            lineTension: 0.2,
            borderColor: color,
            borderCapStyle: 'butt',
            borderDash: [],
            borderDashOffset: 0.0,
            borderJoinStyle: 'miter',
            pointBorderColor: color,
            pointBackgroundColor: "#fff",
            pointBorderWidth: 1,
            pointHoverRadius: 5,
            pointHoverBackgroundColor: color,
            pointHoverBorderColor: color,
            pointHoverBorderWidth: 2,
            pointRadius: 1,
            pointHitRadius: 10,
            data: data[name],
            spanGaps: false
        }
        return dataset
    });
    this._chart_data = {
        datasets: datasets
    }
}

FeatureChartEngine.prototype._initChart = function () {
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
                        stepSize: this._chart_duration / 4 * 1000.0,
                        displayFormats: {
                            millisecond: 'HH:mm:ss.SSS'
                        }
                    }
                }]
            }
        }
    });
}

FeatureChartEngine.prototype._initEvents = function () {
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

FeatureChartEngine.prototype._addChartData = function (data) {
    var duration = this._chart_duration; // seconds
    this._chart.data.datasets.forEach((dataset) => {
        var n_new = data[dataset.label].length;
        end_ts = 0;
        if (n_new > 0) {
            end_ts = data[dataset.label][n_new - 1]['x'].valueOf() / 1000.0;
        }
        var start_ts = end_ts
        if (dataset.data.length != 0) {
            start_ts = dataset.data[0]['x'].valueOf() / 1000.0;
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

FeatureChartEngine.prototype.updateChartData = function (data) {
    var converted_data = this._convertData(data);
    this._addChartData(converted_data);
}

FeatureChartEngine.prototype.initChartData = function (data) {
    var converted_data = this._convertData(data);
    this._initData(converted_data);
    this._initChart();
}

FeatureChartEngine.prototype._convertData = function (stream) {
    // get feature names
    var names = Object.keys(stream[0])
    names = names.filter(function (name) { return name !== 'START_TIME' && name !== 'STOP_TIME' })
    var converted_data = {}
    var all_values = []
    for (name in names) {
        converted_data[name] = stream.map(function (sample) { return { x: moment.unix(sample['START_TIME']), y: sample[name] } });
        all_values.concat(converted_data[name].map(function (x) { return x['y'] }))
    }
    var current_max = ss.max(all_values)
    return converted_data;
}

FeatureChartEngine.prototype.connect = function () {
    var engine = this;
    // register callback when receiving data from worker
    this._worker.onmessage = function (e) {
        if (e.data['action'] == 'error') {
            engine._worker.terminate();
            engine.disconnect();
        } else if (e.data['action'] == 'data') {
            if (e.data.content && e.data.content.length > 0) {
                // console.log('Receiving data buffer of size: ' + e.data.content.length);
                if (this._chart === undefined) {
                    engine.initChartData(e.data.content);
                } else {
                    engine.updateChartData(e.data.content);
                }
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

FeatureChartEngine.prototype.disconnect = function () {
    this._worker.postMessage({
        'action': 'stop',
        'port': this._port
    });
}