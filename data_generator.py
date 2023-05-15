import boto3
import csv
import json
import requests

# Set up the API Gateway client
client = boto3.client('apigateway',region_name='us-east-1')

# Define a mapping of column names to new names
column_map = {
    'Event ID': 'event_id',
    'Event Name': 'event_name',
    'Start Date/Time': 'start_date',
    'End Date/Time': 'end_date',
    'Event Agency': 'event_agency',
    'Event Type': 'event_type',
    'Event Borough': 'event_borough',
    'Event Location': 'event_location',
    'Event Street Side': 'event_street_side',
    'Street Closure Type': 'street_closure_type',
    'Community Board': 'community_board',
    'Police Precinct': 'police_precinct'
}

# Read the CSV file and send the data to the API endpoint in batches of 100
with open('data.csv', 'r') as file:
    reader = csv.DictReader(file)
    for i, row in enumerate(reader):
        # Rename the columns in the row using the column_map
        renamed_row = {}
        for old_name, new_name in column_map.items():
            renamed_row[new_name] = row[old_name]

        # Add the renamed row to the batch
        # Convert the batch to a JSON object
        data = json.dumps(renamed_row)
        #print(type(data))
        # Send a POST request to the API endpoint
        #url = f'https://{api_id}.execute-api.us-east-1.amazonaws.com/prod{resource_path}'
        url = f'	https://8pilbts3xc.execute-api.us-east-1.amazonaws.com/initial/api'
        headers = {'Content-Type': 'application/json'}
        #data1 = {"event_id": "687277", "event_name": "Covid -19 testing site", "start_date": "2/2/2023 8:00", "end_date": "2/2/2023 18:00", "event_agency": "Parks Department", "event_type": "Special Event", "event_borough": "Manhattan", "event_location": "Columbus Park: Columbus Park", "event_street_side": "", "street_closure-type": "N/A", "community_board": "1,", "police_precinct": "5,"}
        response = requests.post(url, headers=headers, data=(data))

        # Print the response
        #print(response.text)
        print(response.status_code)