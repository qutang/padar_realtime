"""

Implementing async process of a function running on a different CPU core for computationally intensive operation

Author: Qu Tang

Date: Jul 05, 2018

Reference:

https://pymotw.com/3/asyncio/executors.html

"""
import concurrent.futures
import asyncio
from multiprocessing import cpu_count
import time
from random import random
import logging
import os


logger = logging.getLogger()

class AsyncMultiProcess:
    def __init__(self, n_cores=None):
        if n_cores is None:
            n_cores = cpu_count()
        self._executor = concurrent.futures.ProcessPoolExecutor(n_cores)
    
    async def run(self, list_of_funcs, list_of_args):
        loop = asyncio.get_event_loop()
        tasks = map(lambda pair: loop.run_in_executor(self._executor, pair[0], *(pair[1])), zip(list_of_funcs, list_of_args))
        gathered_tasks = asyncio.gather(*tasks)
        return gathered_tasks

    @staticmethod
    def default_function(x, times):
        n = 0
        result = 0
        while n < 10:
            time.sleep(x)
            d = random() * times
            print(str(os.getpid()) + ":" + str(d))
            result = result + d
            n = n + 1
        return result

if __name__ == '__main__':
    
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='PID %(process)5s %(name)18s: %(message)s',
        stream=sys.stdout
    )
    def example1():
        print('Run multiple processes on different cores simultaneously in a single call')
        print(str(os.getpid()))
        p = AsyncMultiProcess(n_cores=2)
        asyncio.get_event_loop().run_until_complete(p.run(
            [AsyncMultiProcess.default_function,    AsyncMultiProcess.default_function],
            [[1, 3], [3, -1]]
            ))
        asyncio.get_event_loop().run_forever()

    def example2():
        print('Run multiple processes on different cores sequentially in multiple calls')
        print(str(os.getpid()))
        p = AsyncMultiProcess(n_cores=2)
        result1 = asyncio.get_event_loop().run_until_complete(p.run([AsyncMultiProcess.default_function], [[1, 3]]))
        result1.add_done_callback(lambda result: print(result1.result()))
        result2 = asyncio.get_event_loop().run_until_complete(p.run([AsyncMultiProcess.default_function], [[3, -1]]))
        result2.add_done_callback(lambda result: print(result2.result()))
        asyncio.get_event_loop().run_forever()
    example2()