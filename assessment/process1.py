import pandas as pd
from datetime import datetime
import argparse

parser = argparse.ArgumentParser(description='Key-Value Pair Parser')
parser.add_argument('--to_process', type=str)
parser.add_argument('--output_dir', type=str)
args = parser.parse_args()

input_file = args.to_process
output_file_path = args.output_dir

def get_time_difference(current_time_stamp, prev_time_stamp):
    time_format = '%Y-%m-%dT%H:%M:%SZ'
    current_time = datetime.strptime(current_time_stamp, time_format)
    prev_time = datetime.strptime(prev_time_stamp, time_format)
    time_difference =  current_time - prev_time
    time_diff_in_hours = time_difference.total_seconds() / (60 * 60)
    return time_diff_in_hours

def trip_to_csv(trip_data, file_name):
    df = pd.DataFrame(trip_data, columns=["latitude", "longitude", "timestamp"])
    df.to_csv(f'{output_file_path}/{file_name}.csv', index=False)

raw_data = pd.read_parquet(input_file)

trip_unit = None
previous_timestamp = None
trip_number = 0

trip_data = []

for idx, data in raw_data.iterrows():
    if previous_timestamp == None:
        previous_timestamp = data['timestamp']
        trip_unit = data['unit']
        trip_data.append(data)
    else:
        current_timestamp = data['timestamp']
        if data['unit'] != trip_unit:
            trip_unit = data['unit']
            trip_number = 0
            trip_data.clear()
            trip_data.append(data)
            previous_timestamp = current_timestamp
            continue

        time_difference = get_time_difference(current_timestamp, previous_timestamp)
        if time_difference > 7:
            file_name = f'{trip_unit}_{trip_number}'
            trip_to_csv(trip_data, file_name)
            trip_number += 1
            trip_data.clear()
        previous_timestamp = current_timestamp
        trip_data.append(data)

if len(trip_data) > 0:
    last_file_name = f'{trip_unit}_{trip_number}'
    trip_to_csv(trip_data, last_file_name)