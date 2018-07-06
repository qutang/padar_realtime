"""
Computation engine based on websocket protocol

1. It uses websocket as data receiving and broadcasting socket
2. It uses subprocess for CPU heavy computation
3. It uses coroutine for data buffering

Author: Qu Tang
Date: Jul 05, 2018
"""

import asyncio

class ComputationEngine:
    def __init__(self, b_url, b_port, r_url, r_port):
        self._b_url = b_url
        self._b_port = b_port
        self._r_url = r_url
        self._r_port = r_port
