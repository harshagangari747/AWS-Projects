import json
import boto3
import os

bedrock_model_id = os.environ.get('model_id')
bedrock_client = boto3.client('bedrock-runtime', 'us-west-2')

def lambda_handler(event, context):
    print('triggered scoring event',event)

    resumeData = event['resumeText']
    jobDescription = event['jobDetails']['Items'][0]

    prompt= f""" You are an experienced HR professional. Your task is to evaluate a resume over a given job description. 
    Return only the score in percentage ranging between 0 to 100. This score indicates how well the resume is aligned to the
    given job description.  NO PREAMBLE, EXPLANATION OR ADDITIONAL TEXT.
    ### RESUME:  {resumeData} ### JOB DESCRIPTION  {jobDescription}.
    The output should be like 'score:78' for example.
    """

    formatted_prompt = (
        "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n"
        f"{prompt}"
        "<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n"
    )

    body = {
        "prompt": formatted_prompt,
        "max_gen_len": 20,
        "temperature": 0.2,
        "top_p": 0.8
    }

    try:
       
        response = bedrock_client.invoke_model(
            modelId=bedrock_model_id,
            accept='application/json',
            contentType='application/json',
            body=json.dumps(body)
        )

        response_body = json.loads(response.get('body').read())
        print('ai response', response_body, type(response_body))

        score = response_body.get('generation')
        print('score before', score)


        score = score.split(':')[1].strip()

        print('score after', score)



        output_data = {
        "score": int(score),
        "emailId": event['emailId'],
        "resumeFile": event['resumeFile'],
        "jobId": event['jobId'],
        }

        return output_data

    except Exception as e:
        print('error', e)
    
    