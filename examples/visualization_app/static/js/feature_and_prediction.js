function ArPipelineEngine(feature_chart_ids, url, port, refresh_rate, window_size) {
    this._initIncomeSourceVariables(url, port, refresh_rate);
    this._initFeatureCharts(feature_chart_ids, window_size * 6);
    this._initEvents();
}

ArPipelineEngine.prototype._initIncomeSourceVariables = function (url, port, refresh_rate) {
    this._url = url;
    this._port = port;
    this._refresh_rate = refresh_rate;
    this._worker = new Worker('static/webworker/ar_stream_handler.js');
}

ArPipelineEngine.prototype._initFeatureCharts = function (ids, chart_duration) {
    this._feature_charts = {}
    this._feature_chart_mapping = {}
    var engine = this;
    ids.forEach(function (id, index) {
        engine._feature_charts[id] = new FeatureChart(id, chart_duration);
        engine._feature_chart_mapping[id] = undefined;
    });
    this._common_max_value = 0
    this._common_min_value = 0
}

ArPipelineEngine.prototype._initEvents = function () {
    // register connect event
    var engine = this;
    $('#feature-prediction-connect').click(function () {
        if ($(this).text() === 'Connect') {
            engine.connect();
            $(this).text('Disconnect');
        } else if ($(this).text() === 'Disconnect') {
            engine.disconnect();
            $(this).text('Connect');
        }
    });
}


ArPipelineEngine.prototype.connect = function () {
    var engine = this;
    // register callback when receiving data from worker
    this._worker.onmessage = function (e) {
        if (e.data['action'] == 'error') {
            engine._worker.terminate();
            engine.disconnect();
        } else if (e.data['action'] == 'data') {
            if (e.data.content && e.data.content.length > 0) {
                console.log(e.data.content);
                engine.convertData(e.data.content);
                engine.startFeatureCharts();
                engine.syncFeatureCharts();
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



ArPipelineEngine.prototype.disconnect = function () {
    this._worker.postMessage({
        'action': 'stop',
        'port': this._port
    });
}

ArPipelineEngine.prototype.convertData = function (stream) {
    var fields = Object.keys(stream[0])

    this._feature_data = {}
    this._prediction_data = {}
    var engine = this;

    fields.forEach(function (field, index) {
        if (field.includes('START_TIME') || field.includes('STOP_TIME') || field.includes('INDEX')) {
            // TODO: broadcast and finally add vertical lines to raw data chart
        } else if (field.includes('FEATURE')) {
            var stream_name = field.split('_')[0]
            engine._feature_data[stream_name] = {}
            var names = Object.keys(stream[0][field][0]);
            var feature_names = names.filter(function (name) { return name !== 'START_TIME' && name !== 'STOP_TIME' && !name.endsWith('PREDICTION') })

            feature_names.forEach(function (name) {
                var display_name = name.split(stream_name + '_')[1];
                engine._feature_data[stream_name][display_name] = stream.map(function (chunk) { return { x: moment.unix(chunk[field][0]['STOP_TIME'] / 1000.0).utc(), y: chunk[field][0][name] } });
            });
        } else if (field.includes('PREDICTION')) {
            var task_name = field.split('_')[0]
            engine._prediction_data[task_name] = {}
            var names = Object.keys(stream[0][field][0]);
            var class_names = names.filter(function (name) { return name.endsWith('PREDICTION') })
            class_names.forEach(function (name) {
                var display_name = name.split('_' + task_name)[0]
                engine._prediction_data[task_name][display_name] = stream.map(function (chunk) { return { x: moment.unix(chunk[field][0]['STOP_TIME'] / 1000.0).utc(), y: sample[field][0][name] } });
            });
        } else {
            throw new Error('Unrecognized filed: ' + field)
        }
    })

    return;
}

ArPipelineEngine.prototype.startFeatureCharts = function () {
    var stream_names = Object.keys(this._feature_data);
    var chart_names = Object.keys(this._feature_charts);
    var engine = this;
    chart_names.forEach(function (chart_name, index) {
        var chart = engine._feature_charts[chart_name];
        if (engine._feature_chart_mapping[chart_name] === undefined) {
            engine._feature_chart_mapping[chart_name] = stream_names[index]
        }
        var stream_name = engine._feature_chart_mapping[chart_name]
        var data = engine._feature_data[stream_name]
        if (chart.isInitialized()) {
            chart.updateChartData(data);
        } else {
            chart.initChartData(data);
        }
    });
}

ArPipelineEngine.prototype.syncFeatureCharts = function () {
    var common_max_value = 0;
    var common_min_value = 0;
    var chart_names = Object.keys(this._feature_charts);
    var engine = this;
    chart_names.forEach(function (chart_name, index) {
        var chart = engine._feature_charts[chart_name];
        var data_range = chart.getDataRange();
        console.log(data_range);
        if (data_range[0] < common_min_value) common_min_value = data_range[0];
        if (data_range[1] > common_max_value) common_max_value = data_range[1];
    });
    chart_names.forEach(function (chart_name, index) {
        var chart = engine._feature_charts[chart_name];
        console.log([common_min_value, common_max_value])
        chart.updateYRange(Math.floor(common_min_value * 10.0) / 10.0, Math.ceil(common_max_value * 10.0) / 10.0);
    })
}