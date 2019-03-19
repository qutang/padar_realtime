from padar_realtime.processor_stream import ProcessorStreamManager
import asyncio
import pandas as pd
import os
import signal


def saver(merged):
    def _save(df, output_file):
        if df.empty:
            return
        df['HEADER_TIME_STAMP'] = df['HEADER_TIME_STAMP'].map(
            lambda x: pd.Timestamp.fromtimestamp(x))
        if 'HEADER_TIME_STAMP_REAL' in df:
            df['HEADER_TIME_STAMP_REAL'] = df['HEADER_TIME_STAMP_REAL'].map(
                lambda x: pd.Timestamp.fromtimestamp(x))
        if 'HEADER_TIME_STAMP_ORIGINAL' in df:
            df['HEADER_TIME_STAMP_ORIGINAL'] = df[
                'HEADER_TIME_STAMP_ORIGINAL'].map(
                    lambda x: pd.Timestamp.fromtimestamp(x))
        if 'HEADER_TIME_STAMP_NOLOSS' in df:
            df['HEADER_TIME_STAMP_NOLOSS'] = df[
                'HEADER_TIME_STAMP_NOLOSS'].map(
                    lambda x: pd.Timestamp.fromtimestamp(x))
        if not os.path.exists(output_file):
            df.to_csv(output_file, index=False, float_format='%.3f', mode='w')
        else:
            df.to_csv(
                output_file,
                index=False,
                float_format='%.3f',
                mode='a',
                header=False)

    data_type = merged['DATA_TYPE']
    st = pd.Timestamp.fromtimestamp(merged['MERGED_CHUNK_ST'])
    et = pd.Timestamp.fromtimestamp(merged['MERGED_CHUNK_ET'])
    count = merged['MERGED_CHUNK_INDEX']
    os.makedirs('outputs', exist_ok=True)

    print('')
    print(data_type + ' merged chunk ' + str(count) + ' includes ' +
          str(merged['N_STREAMS']) + ' stream from ' + str(st) + ' to ' +
          str(et))
    print('')

    for chunk in merged['CHUNKS']:
        id = chunk.get_device_id().replace(':', '')
        name = chunk.get_stream_name()
        output_file = 'outputs/' + id + '.' + name + '.' + data_type + '.csv'
        packages = chunk.get_packages()
        dfs = [package.to_dataframe() for package in packages]
        if len(dfs) > 0:
            data_df = pd.concat(dfs, axis=0, ignore_index=True)
            data_df['INDEX'] = count
            data_df['MERGED_CHUNK_ST'] = st
            data_df['MERGED_CHUNK_ET'] = et
            lock = output_file + '.lock'
            try:
                while os.path.exists(lock):
                    pass
                with open(lock, 'w'):
                    pass
                _save(data_df, output_file)
            except Exception as e:
                print(e)
            finally:
                if os.path.exists(lock):
                    os.remove(lock)
        else:
            print(data_type + ' chunk ' + str(count) + ' from ' + id +
                  ' is empty')
    return


if __name__ == '__main__':
    import sys
    names = sys.argv[1:]
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    loop = asyncio.get_event_loop()
    stream_manager = ProcessorStreamManager(
        loop=loop,
        init_input_port=8000,
        init_output_port=9000,
        window_size=3,
        update_rate=3)
    stream_manager.add_processor_stream(
        saver, host='localhost', ws_server=False)
    for name in names:
        stream_manager.add_input_stream(name=name, host='localhost')
    stream_manager.start()
