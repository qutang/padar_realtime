from padar_features.feature_set import FeatureSet
import pandas as pd
import signal
import asyncio
from padar_realtime.processor_stream import ProcessorStreamManager
from functools import reduce
import os
import json
import pickle
import numpy as np


def muss_pipeline(merged, model_files):
    os.makedirs('outputs', exist_ok=True)
    data_type = merged['DATA_TYPE']
    st = merged['MERGED_CHUNK_ST']
    et = merged['MERGED_CHUNK_ET']
    count = merged['MERGED_CHUNK_INDEX']
    stream_names = merged['STREAM_NAMES']
    chunks = merged['CHUNKS']
    if len(chunks) < 1:
        return None
    if data_type != 'accel':
        return None
    else:
        feature_json, feature_df = _compute_features(chunks)
        if feature_json is not None:
            prediction_json, prediction_df = _make_predictions(
                feature_df, model_files)
            _save_features(feature_df, stream_names)
            _save_predictions(prediction_df, stream_names)
            result_json = {**feature_json, **prediction_json}
            result_json['START_TIME'] = st
            result_json['STOP_TIME'] = et
            result_json['INDEX'] = count
            json_str = json.dumps(result_json)
        else:
            json_str = None
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


def _save_predictions(prediction_df, stream_names):
    output_file = 'outputs/' + '-'.join(stream_names) + '.prediction.csv'
    if not os.path.exists(output_file):
        prediction_df.to_csv(
            output_file, index=False, float_format='%.3f', mode='w')
    else:
        prediction_df.to_csv(
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
        if len(packages) < 2:
            return None, None
        dfs = [package.to_dataframe() for package in packages]
        data_df = pd.concat(dfs, axis=0, ignore_index=True)
        clean_df = data_df[['X', 'Y', 'Z']]
        X = clean_df.values
        df = FeatureSet.location_matters(X, 50)
        columns = df.columns
        columns = [col + '_' + stream_name.upper() for col in columns]
        df.columns = columns
        df['START_TIME'] = pd.Timestamp.fromtimestamp(chunk.get_chunk_st())
        df['STOP_TIME'] = pd.Timestamp.fromtimestamp(chunk.get_chunk_et())
        feature_dfs.append(df)
        feature_json[stream_name.upper() + '_FEATURE'] = json.loads(
            df.to_json(orient='records'))

    feature_df = reduce(lambda x, y: x.merge(y), feature_dfs)
    return feature_json, feature_df


def _make_predictions(feature_df, model_files):
    indexed_feature_df = feature_df.set_index(['START_TIME', 'STOP_TIME'])
    prediction_json = {}
    prediction_dfs = []
    for model_file in model_files:
        with open(model_file, 'rb') as mf:
            model_bundle = pickle.load(mf)
            feature_order = model_bundle['feature_order']
            ordered_df = indexed_feature_df.loc[:, feature_order]
            X = ordered_df.values
            name = model_bundle['name']
            class_labels = model_bundle['model'].classes_
            p_df = pd.DataFrame()
            try:
                scaled_X = model_bundle['scaler'].transform(X)
                scores = model_bundle['model'].predict_proba(scaled_X)[0]
            except Exception as e:
                print(str(e))
                scores = len(class_labels) * [np.nan]
            for class_label, score in zip(class_labels, scores):
                p_df[name + '_' + class_label.upper() + '_PREDICTION'] = [
                    score
                ]
            p_df['START_TIME'] = feature_df['START_TIME']
            p_df['STOP_TIME'] = feature_df['STOP_TIME']
            prediction_dfs.append(p_df)
            prediction_json[name + '_PREDICTION'] = json.loads(
                p_df.to_json(orient='records'))
    prediction_df = reduce(lambda x, y: x.merge(y), prediction_dfs)
    return prediction_json, prediction_df


if __name__ == '__main__':
    import sys

    num_of_streams = int(sys.argv[1])
    dir_path = os.path.dirname(os.path.realpath(__file__))
    model_files = [
        os.path.join(dir_path, f) for f in [
            'DW_DA_DT.MO.posture_model.pkl', 'DW_DA_DT.MO.activity_model.pkl',
            'DW_DA_DT.MO.classic_seven_activities_model.pkl'
        ]
    ]
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    loop = asyncio.get_event_loop()
    stream_manager = ProcessorStreamManager(
        loop=loop,
        init_input_port=8000,
        init_output_port=9000,
        window_size=12.8,
        update_rate=1)
    stream_manager.add_processor_stream(
        muss_pipeline,
        host='localhost',
        ws_server=True,
        model_files=model_files)
    for i in range(0, num_of_streams):
        stream_manager.add_input_stream(host='localhost')
    stream_manager.start()