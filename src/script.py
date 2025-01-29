import pandas as pd
import os

def process_brainwaves_data(input_file, id):
    # Read the input CSV file
    df = pd.read_csv(input_file)

    # Fixing Timestamp
    df['timestamp_delta'] = df['timestamp'] - df['laptop_timestamp']
    df['timestamp'] = df['timestamp'] - 0.6
    df.drop(columns=['timestamp_delta', 'laptop_timestamp'], inplace=True)

    # Making Signal quality consolidated
    def map_quality(value):
        if value <= 1.5:
            return 'bad'
        elif value <= 2.5:
            return 'good'
        else:
            return 'great'

    cols = ['SQ_CP3', 'SQ_C3', 'SQ_F5', 'SQ_PO3', 'SQ_PO4', 'SQ_F6', 'SQ_C4', 'SQ_CP4']
    quality_mapping = {'bad': 1, 'good': 2, 'great': 3}

    for col in cols:
        df[col] = df[col].map(quality_mapping)
    
    df['Average_Quality_Value'] = df[cols].mean(axis=1)
    df['Signal_Quality'] = df['Average_Quality_Value'].apply(map_quality)
    df = df.drop(columns=['Average_Quality_Value'] + cols)

    # Separating PowerByBand readings
    pbd_df = df[df['type'] == 'powerByBand'].dropna(axis=1).drop(columns=['type'])
    cols = ['participantName', 'Signal_Quality'] + [col for col in pbd_df.columns if col not in ['participantName', 'Signal_Quality']]
    pbd_df = pbd_df[cols]

    pbd_df['timestamp_delta'] = pbd_df['timestamp'] - pbd_df['timestamp'].shift(1)
    for i in range(2):
        session_id = 1
        session_counter = 0
        def assign_session_id(row):
            nonlocal session_id, session_counter
            if row['timestamp_delta'] > 300 or session_counter >= 4:
                session_id += 1
                session_counter = 0
            session_counter += 1
            return session_id

        pbd_df['session_id'] = pbd_df.apply(assign_session_id, axis=1)
        session_counts = pbd_df['session_id'].value_counts()
        valid_sessions = session_counts[session_counts >= 4].index
        pbd_df = pbd_df[pbd_df['session_id'].isin(valid_sessions)]

    pbd_df.set_index('session_id', inplace=True)
    pbd_df.drop(columns=['timestamp_delta'], inplace=True)
    pbd_df.to_csv(f'../data/pbb/power_by_band_{id}.csv')
    

    # Separating raw readings
    raw_df = df[df['type'] == 'raw'].dropna(axis=1).drop(columns=['type'])
    cols = ['participantName', 'Signal_Quality'] + [col for col in raw_df.columns if col not in ['participantName', 'Signal_Quality']]
    raw_df = raw_df[cols]

    raw_df['timestamp_delta'] = raw_df['timestamp'] - raw_df['timestamp'].shift(1)
    for i in range(2):
        session_id = 1
        session_counter = 0
        def assign_session_id(row):
            nonlocal session_id, session_counter
            if row['timestamp_delta'] > 64 or session_counter >= 256:
                session_id += 1
                session_counter = 0
            session_counter += 1
            return session_id

        raw_df['session_id'] = raw_df.apply(assign_session_id, axis=1)
        session_counts = raw_df['session_id'].value_counts()
        valid_sessions = session_counts[session_counts >= 256].index
        raw_df = raw_df[raw_df['session_id'].isin(valid_sessions)]

    raw_df.set_index('session_id', inplace=True)
    raw_df.drop(columns=['timestamp_delta'], inplace=True)
    raw_df.to_csv(f'../data/raw/raw_{id}.csv')

# Example usage
data_dir = "../data/uncleaned/"
for filename in os.listdir(data_dir):
    if filename.endswith(".csv"):
        file_path = os.path.join(data_dir, filename)
        file_id = os.path.splitext(filename)[0].split('_')[-1]
        process_brainwaves_data(file_path, file_id)
        print(f'ID: {file_id} | {file_path} processed successfully')