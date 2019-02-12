var DataReceiver = function (url, port, rate) {
    this._url = url;
    this._port = port;
    this._rate = rate;
    this._data_buffer = [];
    var thisthis = this;
    var post_data = function () {
        var msg = {
            action: 'data',
            content: thisthis._data_buffer
        }
        // console.log('Sending data to main thread...');
        // console.log(thisthis._data_buffer);
        postMessage(msg);
        clear_buffer();
    };
    var clear_buffer = function () {
        thisthis._data_buffer = [];
    };

    this.run = function () {
        var socket_addr = 'ws://' + url + ':' + port;
        this._socket = new WebSocket(socket_addr);
        this._socket.onopen = function (e) {
            console.log('Connected to ' + socket_addr);
            thisthis._refresher = setInterval(post_data, thisthis._rate * 1000);
        };
        this._socket.onerror = function (e) {
            console.log(e);
            this.close();
            close();
        };
        this._socket.onmessage = function (e) {
            var data = JSON.parse(e.data);
            thisthis._data_buffer.push(data);
        }
    };
    this.stop = function () {
        clearInterval(this._refresher);
        this._refresher = null;
        this._socket.close();
        post_data();
    }
}

var data_receivers = [];

onmessage = function (e) {
    if (e.data['action'] == 'start') {
        console.log('Message received from main script');
        var url = e.data['url'];
        var port = e.data['port'];
        var rate = e.data['rate'];
        console.log('Received: ' + url + ", " + port + ", " + rate);
        var selected_receiver = data_receivers.filter(function (receiver) {
            return receiver._port == e.data['port']
        });
        if (selected_receiver.length == 0) {
            console.log('Server not found, create a new one for: ' + port);
            selected_receiver = new DataReceiver(url, port, rate);
            data_receivers.push(selected_receiver);
        } else {
            selected_receiver = selected_receiver[0];
            console.log('Found a server for: ' + port);
        }
        console.log('Total data receivers: ' + data_receivers.length);
        selected_receiver.run();
    } else if (e.data['action'] == 'stop') {
        var selected_receiver = data_receivers.filter(function (receiver) {
            return receiver._port == e.data['port']
        });
        if (selected_receiver.length == 0) {
            console.log('Server not found during stopping')
        } else {
            selected_receiver[0].stop();
            console.log('stop server: ' + selected_receiver[0]._port)
        }
    }
}

onerror = function (e) {
    var msg = {
        action: 'error',
        content: e
    };
    postMessage(msg);
    console.log('Error on worker');
    console.log(e)
    close();
}