import asyncio
import websockets


async def hello():
    async with websockets.connect(
            'ws://wockets.ccs.neu.edu:8000') as websocket:
        while True:
            data = await websocket.recv()
            print(data)


asyncio.get_event_loop().run_until_complete(hello())