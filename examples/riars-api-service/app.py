import flask
from flask import request, jsonify, abort
import scan_devices as sd
import init_devices as init
import asyncio


class RiarsApi(object):
    def __init__(self, debug=True):
        self._version = '1.0.0'
        self._app = flask.Flask(__name__)
        self._app.config['DEBUG'] = debug
        self._app.add_url_rule(
            '/', 'Lastest Doc', self.doc_page, methods=['GET'])
        self._app.add_url_rule(
            '/api/v<string:version>/', 'Doc', self.doc_page, methods=['GET'])
        self._app.add_url_rule(
            '/api/v<string:version>/sensors/scan/<int:number_of_devices>',
            'Scan devices',
            self.scan_devices,
            methods=['GET'])
        self._app.add_url_rule(
            '/api/v<string:version>/sensors/init/',
            'Init devices',
            self.init_devices,
            methods=['POST'])

    def doc_page(self, version=None):
        if version is None:
            version = self._version
        return '''<h1>RIARS API Service v%s</h1>
        <p>APIs for the real-time interactive AR system.</p>''' % version

    def scan_devices(self, version=None, number_of_devices=1):
        if version.startswith('1'):
            return sd.scan_devices_v1(number_of_devices)
        else:
            return sd.scan_devices_v1(number_of_devices)

    def init_devices(self, version=None):
        if not request.json:
            abort(400)
        else:
            if version.startswith('1'):
                return init.init_devices_v1(request.json,
                                            asyncio.new_event_loop()), 200
            else:
                return init.init_devices_v1(request.json,
                                            asyncio.new_event_loop()), 200

    def start(self):
        self._app.run(
            host='localhost', port=5000, debug=self._app.config['DEBUG'])


if __name__ == '__main__':
    debug = True
    app = RiarsApi(debug=debug)
    app.start()