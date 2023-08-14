def lambda_handler(event, context):
    import boto3
    forecast = boto3.client('forecast')

    response = forecast.create_dataset(
        DatasetName='workshop_timeseries_retail_target',
        Domain='RETAIL',
        DatasetType='TARGET_TIME_SERIES',
        DataFrequency='D',
        Schema={
            "Attributes": [
                {
                  "AttributeName": "item_id",
                  "AttributeType": "string"
                },
                {
                 "AttributeName": "timestamp",
                 "AttributeType": "timestamp"
                },
                {
                 "AttributeName": "demand",
                 "AttributeType": "float"
                }
            ]
            },
    )

    event['DatasetArn'] = response['DatasetArn']

    response = forecast.update_dataset_group(
        DatasetGroupArn=event['DatasetGroupArn'],
           DatasetArns=[
            event['DatasetArn'],
        ]
    )
    
    return event
