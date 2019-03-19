from padar_features.feature_set import FeatureSet
import pandas as pd
import signal
import asyncio
from padar_realtime.processor_stream import ProcessorStreamManager


def compute_features(chunk):
    if chunk.get_data_type() != 'accel':
        feature_df = pd.DataFrame()
    else:
        packages = chunk.get_packages()
        dfs = [package.to_dataframe() for package in packages]
        data_df = pd.concat(dfs, axis=0, ignore_index=True)
        clean_df = data_df[['X', 'Y', 'Z']]
        X = clean_df.values
        feature_df = FeatureSet.location_matters(X, 50)
        feature_df['START_TIME'] = pd.Timestamp.fromtimestamp(
            chunk.get_chunk_st())
        feature_df['STOP_TIME'] = pd.Timestamp.fromtimestamp(
            chunk.get_chunk_et())
    print(feature_df)
    return feature_df


if __name__ == '__main__':
    import sys
    name = 'test'
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
    stream_manager.add_input_stream(name=name, host='localhost')
    stream_manager.start()