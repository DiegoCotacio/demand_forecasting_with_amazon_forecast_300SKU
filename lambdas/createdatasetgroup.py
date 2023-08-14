def lambda_handler(event, context):
    import boto3
    forecast = boto3.client('forecast')

    response = forecast.create_dataset_group(
        DatasetGroupName='workshop_timeseries_retail',
        Domain='RETAIL',
    )
    event['DatasetGroupArn'] = response['DatasetGroupArn']
    
    return event