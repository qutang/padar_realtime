import threading
import queue
import os
import logging


class FileStream(object):
    def __init__(self, filename):
        self._filename = filename
        self._df_to_write = queue.Queue()
        self._stop_sign = None

    def writer(self):
        if os.path.exists(self._filename):
            mode = 'a'
            header = False
        else:
            mode = 'w'
            header = True
        while True:
            df = self._df_to_write.get()
            if df is None:
                break
            with open(self._filename, mode) as f:
                df.to_csv(f, header=header, index=False)

    def start(self):
        threading.Thread(target=self.writer).start()
        print('File stream started')

    def write(self, df):
        self._df_to_write.put(df)

    def stop(self):
        self._df_to_write.put(self._stop_sign)


if __name__ == "__main__":
    import pandas as pd
    import numpy as np
    df = pd.DataFrame(np.random.randn(5, 3), columns=list('ABC'))
    streamer = FileStream('test.csv')
    streamer.start()
    streamer.write(df)
    streamer.stop()
