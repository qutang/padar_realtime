from padar_features.feature_set import FeatureSet
import pandas as pd
import signal
import asyncio
from padar_realtime.processor_stream import ProcessorStreamManager
from functools import reduce
import os


def compute_features(merged):
    os.makedirs('outputs', exist_ok=True)
    data_type = merged['DATA_TYPE']
    merged['STREAM_NAMES'].sort()
    output_file = 'outputs/' + '-'.join(
        merged['STREAM_NAMES']) + '.feature.csv'
    if data_type != 'accel':
        feature_df = None
    else:
        feature_dfs = []
        for chunk in merged['CHUNKS']:
            stream_name = chunk.get_stream_name()
            packages = chunk.get_packages()
            dfs = [package.to_dataframe() for package in packages]
            data_df = pd.concat(dfs, axis=0, ignore_index=True)
            clean_df = data_df[['X', 'Y', 'Z']]
            X = clean_df.values
            feature_df = FeatureSet.location_matters(X, 50)
            columns = feature_df.columns
            columns = [stream_name.upper() + '_' + col for col in columns]
            feature_df.columns = columns
            feature_df['START_TIME'] = pd.Timestamp.fromtimestamp(
                chunk.get_chunk_st())
            feature_df['STOP_TIME'] = pd.Timestamp.fromtimestamp(
                chunk.get_chunk_et())
            feature_dfs.append(feature_df)
        feature_df = reduce(lambda x, y: x.merge(y), feature_dfs)

        if not os.path.exists(output_file):
            feature_df.to_csv(
                output_file, index=False, float_format='%.3f', mode='w')
        else:
            feature_df.to_csv(
                output_file,
                index=False,
                float_format='%.3f',
                mode='a',
                header=False)
    if feature_df is not None:
        feature_df_json = feature_df.to_json(orient='records')
    else:
        feature_df_json = None
    return feature_df_json


if __name__ == '__main__':
    import sys
    names = sys.argv[1:]
    names = ['stream1', 'stream2']
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    loop = asyncio.get_event_loop()
    stream_manager = ProcessorStreamManager(
        loop=loop,
        init_input_port=8000,
        init_output_port=9000,
        window_size=12.8,
        update_rate=6.4)
    stream_manager.add_processor_stream(
        compute_features, host='localhost', ws_server=True)
    for name in names:
        stream_manager.add_input_stream(name=name, host='localhost')
    stream_manager.start()