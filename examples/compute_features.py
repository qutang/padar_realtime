def featureset(df, sr):
    df = df.set_index(df.columns[0])
    X = df.values
    freq = spectrum.FrequencyFeature(X, sr=sr)
    freq.fft().peaks()
    ori = orientation.OrientationFeature(X, subwins=4)
    ori.estimate_orientation(unit='deg')

    result_df = pd.concat([
        stats.mean(X),
        stats.std(X),
        stats.positive_amplitude(X),
        stats.negative_amplitude(X),
        stats.amplitude_range(X),
        freq.dominant_frequency(n=1),
        freq.highend_power(),
        freq.dominant_frequency_power_ratio(n=1),
        freq.total_power(),
        ori.median_angles(),
        ori.range_angles(),
        ori.std_angles()
    ],
                          axis=1)

    result_df.insert(0, 'START_TIME', df.index.values[0])
    result_df.insert(1, 'STOP_TIME', df.index.values[-1])
    return result_df