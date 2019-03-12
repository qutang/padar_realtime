from padar_realtime.processor_stream import ProcessorStreamManager
import asyncio
import pandas as pd
import os
import signal


def saver(data):
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

    name = data['name']
    count = data['count']
    os.makedirs('outputs', exist_ok=True)
    accel_output_file = 'outputs/' + name + '.sensor.csv'
    battery_output_file = 'outputs/' + name + '.battery.csv'
    accel_data = list(filter(lambda x: 'X' in x, data['data']))
    battery_data = list(filter(lambda x: 'BATTERY_VOLTAGE' in x, data['data']))
    accel_df = pd.DataFrame(accel_data)
    accel_df['package_index'] = count
    battery_df = pd.DataFrame(battery_data)
    accel_lock = accel_output_file + '.lock'
    try:
        while os.path.exists(accel_lock):
            pass
        with open(accel_lock, 'w'):
            print('create accel lock')
            pass
        _save(accel_df, accel_output_file)
    except Exception as e:
        print(e)
    finally:
        if os.path.exists(accel_lock):
            print('remove accel lock')
            os.remove(accel_lock)

    battery_lock = battery_output_file + '.lock'
    try:
        while os.path.exists(battery_lock):
            pass
        with open(battery_lock, 'w'):
            print('create accel lock')
            pass
        _save(battery_df, battery_output_file)
    except Exception as e:
        print(e)
    finally:
        if os.path.exists(battery_lock):
            print('remove battery lock')
            os.remove(battery_lock)
    return


if __name__ == '__main__':
    import sys
    name = sys.argv[1]
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
    stream_manager.add_input_stream(name=name, host='localhost')
    stream_manager.start()
