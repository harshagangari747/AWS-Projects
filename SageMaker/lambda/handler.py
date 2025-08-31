import json
import boto3

def lambda_handler(event, context):
    runtime = boto3.client('sagemaker-runtime')
    endpoint_name = 'gallstone'

    input_json = json.loads(event['body'])

    response = runtime.invoke_endpoint(
        EndpointName=endpoint_name,
        ContentType='application/json',
        Body=json.dumps(input_json)
    )
    
    result = response['Body'].read().decode('utf-8')
    result = json.loads(result)

    print('result', result, type(result))
    pred_val = result['predictions'][0]
    prob = float(pred_val)

    label = 1 if prob > 0.5 else 0
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'probability': prob,
            'label': label
        })
    }
