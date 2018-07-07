var DataReceiver = function(url, port, rate){
    this._url = url;
    this._port = port;
    this._rate = rate;
    this._data_buffer = [];
    var thisthis = this;
    var post_data = function(){
        var msg = {
            action: 'data',
            content: thisthis._data_buffer
        }
        console.log('Sending data to main thread...');
        console.log(thisthis._data_buffer);
        postMessage(msg);
        clear_buffer();
    };
    var clear_buffer = function(){
        thisthis._data_buffer = [];
    };
    
    this.run = function(){
        var socket_addr = 'ws://' + url + ':' + port;
        this._socket = new WebSocket(socket_addr);
        this._socket.onopen = function(e){
            console.log('Connected to ' + socket_addr);
            thisthis._refresher = setInterval(post_data, thisthis._rate * 1000);
        };
        this._socket.onerror = function(e){
            console.log(e);
            this.close();
            close();
        };
        this._socket.onmessage = function(e){
            var data = JSON.parse(e.data);
            thisthis._data_buffer.push(data);
        }
    };
    this.stop = function(){
        clearInterval(this._refresher);
        this._refresher = null;
        this._socket.close();
        post_data();
        close();
    }
}

var data_receiver;

onmessage = function(e) {
    if(e.data['action'] == 'start'){
        console.log('Message received from main script');
        var url = e.data['url'];
        var port = e.data['port'];
        var rate = e.data['rate'];
        console.log('Received: ' + url + ", " + port + ", " + rate);
        data_receiver = new DataReceiver(url, port, rate);
        console.log(data_receiver);
        data_receiver.run();
    }else if(e.data['action'] == 'stop'){
        data_receiver.stop();
    }
}

onerror = function(e){
    var msg = {
        action: 'error',
        content: e
    };
    postMessage(msg);
    console.log('Error on worker');
    console.log(e)
    close();
}