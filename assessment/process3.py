import os
import json
import pandas as pd
import argparse

parser = argparse.ArgumentParser(description='Key-Value Pair Parser')
parser.add_argument('--to_process', type=str)
parser.add_argument('--output_dir', type=str)
args = parser.parse_args()

process2_output_path = args.to_process
output_file_path = args.output_dir

json_files = os.listdir(process2_output_path)

csv_columns_headers = [
    "unit",
    "trip_id",
    "toll_loc_id_start",
    "toll_loc_id_end",
    "toll_loc_name_start",
    "toll_loc_name_end",
    "toll_system_type",
    "entry_time",
    "exit_time",
    "tag_cost",
    "cash_cost",
    "license_plate_cost"
]

transformed_data = []
for trip in json_files:
    formatted_data = {}
    with open(f'{process2_output_path}/{trip}', 'r') as file:
        trip_data = json.load(file)
        formatted_data['unit'] = trip_data['summary']['share']['uuid']
        formatted_data['trip_id'] = trip.replace('.json', '')
        toll_data = trip_data['route']['tolls']
        for toll in toll_data:
            try:
                formatted_data['toll_loc_id_start'] = toll['start']['id']
                formatted_data['toll_loc_id_end'] = toll['end']['id']
                formatted_data['toll_loc_name_start'] = toll['start']['name']
                formatted_data['toll_loc_name_end'] = toll['end']['name']
                formatted_data['toll_system_type'] = toll['type']
                formatted_data['entry_time'] = toll['start']['arrival']['time']
                formatted_data['exit_time'] =  toll['end']['arrival']['time']
                formatted_data['tag_cost'] = toll['tagCost']
                formatted_data['cash_cost'] = toll['cashCost']
                formatted_data['license_plate_cost'] = toll['licensePlateCost']
            except KeyError:
                continue
    transformed_data.append(formatted_data)

transformed_df = pd.DataFrame(transformed_data, columns=csv_columns_headers)
transformed_df.to_csv(f'{output_file_path}/transformed_data.csv', index=False)