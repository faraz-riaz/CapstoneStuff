import pandas as pd
import os
import warnings

warnings.filterwarnings("ignore")

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
    pbd_df.drop(columns=['timestamp_delta'], inplace=True)

    session_id = 1
    sessions = []

    for i in pbd_df['frequency'].unique():
        filtered_df = pbd_df[pbd_df['frequency'] == i]

        for i in range(0, len(filtered_df) - 3):
            session = filtered_df.iloc[i:i+4]
            if len(session) == 4:
                session['session_id'] = session_id
                sessions.append(session)
                session_id += 1

    sessions_df = pd.concat(sessions)
    sessions_df.reset_index(drop=True, inplace=True)
    sessions_df.set_index('session_id', inplace=True)
    sessions_df.to_csv(f'../data/pbb/power_by_band_{id}.csv')
    

    # Separating raw readings
    raw_df = df[df['type'] == 'raw'].dropna(axis=1).drop(columns=['type'])
    cols = ['participantName', 'Signal_Quality'] + [col for col in raw_df.columns if col not in ['participantName', 'Signal_Quality']]
    raw_df = raw_df[cols]

    session_id = 1
    sessions = []

    for i in raw_df['frequency'].unique():
        filtered_df = raw_df[raw_df['frequency'] == i]

        for i in range(0, len(filtered_df) - 255):
            session = filtered_df.iloc[i:i+256]
            if len(session) == 256:
                session['session_id'] = session_id
                sessions.append(session)
                session_id += 1

    sessions_df = pd.concat(sessions)
    sessions_df.reset_index(drop=True, inplace=True)
    sessions_df.set_index('session_id', inplace=True)
    sessions_df.to_csv(f'../data/raw/raw_{id}.csv')

# Example usage
data_dir = "../data/uncleaned/"
for filename in os.listdir(data_dir):
    if filename.endswith(".csv"):
        file_path = os.path.join(data_dir, filename)
        file_id = os.path.splitext(filename)[0].split('_')[-1]
        process_brainwaves_data(file_path, file_id)
        print(f'ID: {file_id} | {file_path} processed successfully')