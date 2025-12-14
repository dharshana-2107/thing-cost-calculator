import json
import boto3
import math

cloudwatch = boto3.client('cloudwatch')

def lambda_handler(event, context):
    print("Incoming event:", json.dumps(event))
    print("Event type:", type(event))
    print("Event content:", event)


    device_id = event.get('clientId')
    temperature = event.get('temperature')
    pr

    if not device_id or temperature is None:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing deviceId or temperature')
        }
    

    payload_bytes = json.dumps(event).encode('utf-8')
    payload_size_bytes = len(payload_bytes)
    payload_size_increments = math.ceil(payload_size_bytes / 5120)  

    rules_triggered = 1          
    actions_executed = 1        

    cloudwatch.put_metric_data(
        Namespace='IoTDeviceMessages',
        MetricData=[
            {
                'MetricName': 'InboundMessages',
                'Dimensions': [{'Name': 'DeviceId', 'Value': device_id}],
                'Value': 1,
                'Unit': 'Count'
            },
            {
                'MetricName': 'InboundMessageIncrements',
                'Dimensions': [{'Name': 'DeviceId', 'Value': device_id}],
                'Value': payload_size_increments,
                'Unit': 'Count'
            },
            {
                'MetricName': 'RulesTriggered',
                'Dimensions': [{'Name': 'DeviceId', 'Value': device_id}],
                'Value': rules_triggered,
                'Unit': 'Count'
            },
            {
                'MetricName': 'ActionsExecuted',
                'Dimensions': [{'Name': 'DeviceId', 'Value': device_id}],
                'Value': actions_executed,
                'Unit': 'Count'
            }
        ]
    )

    return {
        'statusCode': 200,
        'body': json.dumps(f'Metrics for temperature={temperature} published successfully')
    }

'''test with 
{
  "clientId": "tempsensor009",
  "temperature": 27.0
}'''