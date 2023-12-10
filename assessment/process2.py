from dotenv import load_dotenv
import os
import requests
import json
import argparse

parser = argparse.ArgumentParser(description='Key-Value Pair Parser')
parser.add_argument('--to_process', type=str)
parser.add_argument('--output_dir', type=str)
args = parser.parse_args()

process1_output_path = args.to_process
output_file_path = args.output_dir

load_dotenv()

api_url = os.getenv("TOLLGURU_API_URL")
api_key = os.getenv("TOLLGURU_API_KEY") 

csv_files = os.listdir(process1_output_path)

headers = {'x-api-key': api_key, 'Content-Type': 'text/csv'}
process2_output_path = 'assessment/evaluation_data/output/process2'

for csv in csv_files:
    with open(f'{process1_output_path}/{csv}', 'rb') as file:
        response = requests.post(api_url, data=file, headers=headers)
        output_file_name = csv.replace('.csv', '.json')
        json_file = open(f'{output_file_path}/{output_file_name}', 'w')
        json_file.write(json.dumps(response.json(), indent=4))