function SensorEngine(sensor_chart_ids, url, ports, refresh_rate, window_size) {
    this._initIncomeSourceVariables(url, ports, refresh_rate);
    this._initSensorCharts(sensor_chart_ids, window_size);
    this._initEvents();
}

SensorEngine.prototype._initIncomeSourceVariables = function (url, ports, refresh_rate) {
    this._url = url;
    this._ports = ports;
    this._refresh_rate = refresh_rate;
    this._workers = ports.map(p => new Worker('static/webworker/sensor_stream_handler.js'))
}

SensorEngine.prototype._initSensorCharts = function (ids, chart_duration) {
    this._sensor_charts = {}
    this._sensor_chart_mapping = {}
    var engine = this;
    ids.forEach(function (id, index) {
        engine._sensor_charts[id] = new SensorChart(id, chart_duration);
        engine._sensor_chart_mapping[id] = undefined;
    });
    this._common_max_value = 0
    this._common_min_value = 0
}

SensorEngine.prototype._initEvents = function () {
    // register connect event
    var engine = this;
    $('#sensors-connect').click(function () {
        if ($(this).text() === 'Connect') {
            engine.connect();
            $(this).text('Disconnect');
        } else if ($(this).text() === 'Disconnect') {
            engine._ports.forEach(function (port) {
                engine.disconnect(port);
            });
            $(this).text('Connect');
        }
    });
}


SensorEngine.prototype.connect = function () {
    var engine = this;
    // register callback when receiving data from worker
    this._workers.forEach(function (worker, index) {
        worker.onmessage = function (e) {
            if (e.data['action'] == 'error') {
                worker.terminate();
                engine.disconnect(engine._ports[index]);
            } else if (e.data['action'] == 'data') {
                if (e.data.content && e.data.content.length > 0) {
                    engine.convertData(e.data.content);
                    engine.startSensorCharts();
                }
            }
        };
        worker.postMessage({
            'action': 'start',
            'url': engine._url,
            'port': engine._ports[index],
            'rate': engine._refresh_rate
        });
        console.log('Connecting to ws://' + engine._url + ':' + engine._ports[index] + ' at refresh rate: ' + engine._refresh_rate + ' seconds...');
    });
}


SensorEngine.prototype.disconnect = function (port) {
    var i = this._ports.indexOf(port)
    this._workers[i].postMessage({
        'action': 'stop',
        'port': port
    });
    console.log('Disconnect ' + this._url + ':' + port)
}

SensorEngine.prototype.convertData = function (stream) {
    var sensor_stream = stream.filter(function (sample) {
        return 'accel' === sample['DATA_TYPE']
    });
    var battery_stream = stream.filter(function (sample) {
        return 'battery' === sample['DATA_TYPE']
    });
    this._sensor_data = {}
    var stream_name = stream[0]['STREAM_NAME']
    var stream_order = stream[0]['STREAM_ORDER']
    var sensor_data = this._convertData(sensor_stream);
    if (battery_stream.length > 0) {
        var battery_data = this._convertData(battery_stream);
        this._sensor_data[stream_name] = {
            'order': stream_order,
            'data': { ...sensor_data, ...battery_data }
        };
    } else {
        this._sensor_data[stream_name] = {
            'order': stream_order,
            'data': sensor_data
        }
    }
}


SensorEngine.prototype._convertData = function (data_stream) {
    var result = {}
    var data_fields = Object.keys(data_stream[0]['VALUE'])
    data_fields.forEach(field => {
        result[field] = data_stream.map(sample => {
            return { x: moment.unix(sample['HEADER_TIME_STAMP']), y: sample['VALUE'][field] }
        });
    });
    return result;
}

SensorEngine.prototype.startSensorCharts = function () {
    var stream_name = Object.keys(this._sensor_data)[0];
    var chart_names = Object.keys(this._sensor_charts);
    var stream_order = this._sensor_data[stream_name]['order'];
    var data = this._sensor_data[stream_name]['data'];
    var chart = this._sensor_charts[chart_names[stream_order]];
    if (chart.isInitialized()) {
        chart.updateChartData(data);
    } else {
        chart.initChartData(data, stream_name);
    }
}


SensorEngine.prototype.syncSensorCharts = function () {
    var common_max_value = 0;
    var common_min_value = 1;
    var chart_names = Object.keys(this._sensor_charts);
    var engine = this;
    chart_names.forEach(function (chart_name, index) {
        var chart = engine._sensor_charts[chart_name];
        if (chart.isInitialized()) {
            var data_range = chart.getDataRange();
            console.log(data_range);
            if (data_range[0] < common_min_value) common_min_value = data_range[0];
            if (data_range[1] > common_max_value) common_max_value = data_range[1];
        }
    });
    chart_names.forEach(function (chart_name, index) {
        var chart = engine._sensor_charts[chart_name];
        console.log([common_min_value, common_max_value])
        if (chart.isInitialized()) {
            chart.updateYRange(Math.floor(common_min_value * 10.0) / 10.0, Math.ceil(common_max_value * 10.0) / 10.0);
        }
    });
}