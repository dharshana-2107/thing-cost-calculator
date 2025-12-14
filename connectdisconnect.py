import json
import boto3
import time
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
cloudwatch = boto3.client('cloudwatch')
table = dynamodb.Table('iotdata')

def lambda_handler(event, context):
    print("Received event:")
    print(json.dumps(event, indent=2))

    try:
        device_id = event['clientId']
        event_type = event['eventType']
        MAX_ALLOWED_AGE = 60 * 60 * 24 * 14
        
        try:
            timestamp = int(event['timestamp'])
            now = int(time.time())

            if now - timestamp > MAX_ALLOWED_AGE:
                print(f"Timestamp too old ({timestamp}). Using current time.")
                timestamp = now
        except KeyError:
            timestamp = int(time.time())
            print("No timestamp provided. Using current time.")

        if event_type == 'connected':
            table.put_item(
                Item={
                    'clientId': device_id,
                    'connectTimestamp': timestamp
                }
            )
            print(f"Stored connection timestamp for {device_id}")

        elif event_type == 'disconnected':

            response = table.get_item(Key={'clientId': device_id})
            if 'Item' not in response:
                raise ValueError("No connection timestamp found for client")

            connect_timestamp = float(response['Item']['connectTimestamp'])
            duration_seconds = round((timestamp - connect_timestamp), 2)

            dashboard_response = cloudwatch.put_metric_data(
            Namespace='IoTDeviceMterics',
            MetricData=[
                {
                        'MetricName': 'ConnectivityMinutes',
                        'Dimensions': [
                            {'Name': 'DeviceId', 'Value': device_id}
                        ],
                        'Timestamp': datetime.utcfromtimestamp(timestamp),
                        'Value': duration_seconds,
                        'Unit': 'Seconds'
                    }
                ]
            )
            print(f"Dashboard response: {dashboard_response}")

            

            print(f"Published metric for {device_id}: {duration_seconds} seconds")

          
            table.delete_item(Key={'clientId': device_id})
            print(f"Deleted DynamoDB entry for {device_id}")
            
        else:
            raise ValueError("Unsupported eventType")

    except KeyError as e:
        print(f"Missing field in event: {e}")
        raise ValueError(f"{e} is required in the event")
    except Exception as e:
        print(f"Error: {e}")
        raise
'''test with
{
  "clientId": "tempsensor002",
  "eventType": "connected",
  "timestamp": 1717340000
} for connect 
and
{
  "clientId": "tempsensor002",
  "eventType": "disconnected",
  "timestamp": 1717488992
} for disconnect '''