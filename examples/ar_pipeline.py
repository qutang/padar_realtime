from padar_features.feature_set import FeatureSet
import pandas as pd
import signal
import asyncio
from padar_realtime.processor_stream import ProcessorStreamManager
from functools import reduce
import os
import json


def ar_pipeline(merged):
    os.makedirs('outputs', exist_ok=True)
    data_type = merged['DATA_TYPE']
    st = merged['MERGED_CHUNK_ST']
    et = merged['MERGED_CHUNK_ET']
    count = merged['MERGED_CHUNK_INDEX']
    stream_names = merged['STREAM_NAMES']
    chunks = merged['CHUNKS']
    if data_type != 'accel':
        return None
    else:
        feature_json, feature_df = _compute_features(chunks)
        _save_features(feature_df, stream_names)
        feature_json['START_TIME'] = st
        feature_json['STOP_TIME'] = et
        feature_json['INDEX'] = count
        json_str = json.dumps(feature_json)
        print(json_str)
        return json_str


def _save_features(feature_df, stream_names):
    output_file = 'outputs/' + '-'.join(stream_names) + '.feature.csv'
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


def _compute_features(chunks):
    feature_dfs = []
    feature_json = {}
    for chunk in chunks:
        stream_name = chunk.get_stream_name()
        packages = chunk.get_packages()
        dfs = [package.to_dataframe() for package in packages]
        data_df = pd.concat(dfs, axis=0, ignore_index=True)
        clean_df = data_df[['X', 'Y', 'Z']]
        X = clean_df.values
        df = FeatureSet.location_matters(X, 50)
        columns = df.columns
        columns = [stream_name.upper() + '_' + col for col in columns]
        df.columns = columns
        df['START_TIME'] = pd.Timestamp.fromtimestamp(chunk.get_chunk_st())
        df['STOP_TIME'] = pd.Timestamp.fromtimestamp(chunk.get_chunk_et())
        feature_dfs.append(df)
        feature_json[stream_name.upper() + '_FEATURE'] = json.loads(
            df.to_json(orient='records'))

    feature_df = reduce(lambda x, y: x.merge(y), feature_dfs)
    return feature_json, feature_df


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
        update_rate=1)
    stream_manager.add_processor_stream(
        ar_pipeline, host='localhost', ws_server=True)
    for name in names:
        stream_manager.add_input_stream(name=name, host='localhost')
    stream_manager.start()