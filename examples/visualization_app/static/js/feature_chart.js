function FeatureChart(chart_id, chart_duration) {
    this._chart_colors = {
        red: 'rgb(255, 99, 132)',
        orange: 'rgb(255, 159, 64)',
        yellow: 'rgb(255, 205, 86)',
        green: 'rgb(75, 192, 192)',
        blue: 'rgb(54, 162, 235)',
        purple: 'rgb(153, 102, 255)',
        grey: 'rgb(201, 203, 207)'
    };
    this._chart_id = chart_id;
    this._chart_duration = chart_duration;
    this._chart_index = parseInt(this._chart_id.split('_').pop());
    this._chart_ctx = document.getElementById(this._chart_id).getContext("2d");
    this._initialized = false;
}

FeatureChart.prototype.isInitialized = function () {
    return this._initialized;
}

FeatureChart.prototype._initData = function (data) {
    var names = Object.keys(data);
    var color_names = Object.keys(this._chart_colors);
    var colors = this._chart_colors;
    var datasets = names.map(function (name, index) {
        var color = colors[color_names[index % color_names.length]];
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
            spanGaps: false,
            hidden: true
        }
        return dataset
    });
    this._chart_data = {
        datasets: datasets
    }
}

FeatureChart.prototype._initChart = function () {
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
                        stepSize: this._chart_duration * 1000.0 / 4,
                        displayFormats: {
                            millisecond: 'HH:mm:ss.SSS'
                        }
                    }
                }]
            }
        }
    });
}

FeatureChart.prototype._addChartData = function (data) {
    var duration = this._chart_duration; // seconds
    this._chart.data.datasets.forEach((dataset) => {
        var n_new = data[dataset.label].length;
        var end_ts = 0;
        if (n_new > 0) {
            end_ts = data[dataset.label][n_new - 1]['x'].valueOf() / 1000.0;
        }
        var start_ts = end_ts
        if (dataset.data.length != 0) {
            start_ts = dataset.data[0]['x'].valueOf() / 1000.0;
        }
        if (end_ts - start_ts > duration) { // if there are more than 10s data
            var keep_ts_start = end_ts - duration
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

FeatureChart.prototype.updateChartData = function (data) {
    this._addChartData(data);
}

FeatureChart.prototype.initChartData = function (data) {
    this._initData(data);
    this._initChart();
    this._initialized = true;
}

FeatureChart.prototype.updateYRange = function (min_value, max_value) {
    this._chart.options.scales.yAxes[0].ticks.max = max_value;
    this._chart.options.scales.yAxes[0].ticks.min = min_value;
    this._chart.update({
        duration: 0
    });
}

FeatureChart.prototype.getYRange = function () {
    return [this._chart.scales['y-axis-0'].min, this._chart.scales['y-axis-0'].max];
}

FeatureChart.prototype.getDataRange = function () {
    var common_max_value = 0;
    var common_min_value = 0;
    var chart = this;
    this._chart.data.datasets.forEach((dataset, i) => {
        if (chart._chart.isDatasetVisible(i)) {
            var values = dataset.data.map(function (point) { return point['y'] });
            var max_value = ss.max(values);
            var min_value = ss.min(values);
            if (max_value > common_max_value) common_max_value = max_value;
            if (min_value < common_min_value) common_min_value = min_value;
        }
    });
    console.log([common_min_value, common_max_value])
    return [common_min_value, common_max_value];
}