import websockets
from random import random
import time
import asyncio
import json

clients = set()
counter = 0


async def handler(websocket, path):
    global counter
    print('New client connected')
    while True:
        try:
            clients.add(websocket)
            if counter == 50:
                data = {
                    'HEADER_TIME_STAMP': time.time(),
                    'BATTERY_PERCENTAGE': random()
                }
                value = json.dumps(data)
                for c in clients:
                    await c.send(value)
                counter = 0
            data = {
                'HEADER_TIME_STAMP': time.time(),
                'X': random(),
                'Y': random(),
                'Z': random()
            }
            value = json.dumps(data)
            for c in clients:
                await c.send(value)
            counter = counter + 1
            await asyncio.sleep(1 / 50.0)
        except websockets.exceptions.ConnectionClosed:
            print('client disconnected')
            clients.remove(websocket)
            break


server = websockets.serve(handler, host='*', port=8000)
print('Run websocket server on: ' + 'wockets.ccs.neu.edu: 8000')

asyncio.get_event_loop().run_until_complete(server)
asyncio.get_event_loop().run_forever()
