import json
import boto3
import os
import time
from boto3.dynamodb.conditions import Key, Attr

bucket_name = os.environ['BUCKET']
s3 = boto3.client('s3')

textract_client = boto3.client('textract')

model = os.environ['MODEL']
bedrock_client = boto3.client('bedrock-runtime')

dynamodb = boto3.resource('dynamodb')
table_name = os.environ['DYNAMODB_TABLE']
table = dynamodb.Table(table_name)


def lambda_handler(event, context):

    records = event['Records']
    print('records',records)

    for record in records:
        bucket_name = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print(bucket_name)
        print(key)
        prefix = key.split('/')[0]
        file_name = key.split('/')[1]
        if file_name == 'question_paper.pdf':
            question_paper_json = save_question_paper_dynamodb(bucket_name,key)
            print('question_paper_json', question_paper_json)

        else:
            print('Fetching question paper from dynamodb')
            prefix = key.split('/')[0]
            question_paper_key = prefix + '/question_paper.pdf'
            question_paper_json = get_db_item(question_paper_key)

            evaluate_answersheet(bucket_name, key, question_paper_json)




def save_question_paper_dynamodb(bucket_name,key):

    """
    query dynamodb to see if the question paper exists
    """
    try:
        response = get_db_item(key)

    except Exception as e:
        print('Error while fetching initial dynamodb question paper',e)
        return



    """ 
    run text extract job for question paper
    """
    try:
        extracted_text = extract_text(bucket_name, key)
    except Exception as e:
        print('Error while extracting text from question paper', e)
        return



    """
    Fetch question paper prompt
    """
    try:
        with open('question_paper_prompt.txt', 'r') as file:
            question_paper_prompt = file.read()
        
    except FileNotFoundError:
        print("Question paper prompt text file not found.")
        return


    """
    Call gpt-oss-120B LLM for parsed question paper
    """
    try:
        llm_response = query_llm(question_paper_prompt, extracted_text)

    except Exception as e:
        print('Error while querying LLM...', e)
        return

    """
    Save the question paper to dynamodb
    """
    try:

        if not llm_response:
            print('No question paper generated. Possible because of text might not be extracted properly')
            raise Exception('No question paper generated. Possible because of text might not be extracted properly')
           
        table.put_item(
                Item={
                    'Id': key,
                    'question_paper': llm_response
                }
            )
        print('Question paper saved to dynamodb...')
        return llm_response
    except Exception as e:
        print('Error while saving question paper to dynamodb', e)
        return

def get_db_item(key):
    try:
        print('Fetching initial dynamodb item', key)

        response = table.get_item(Key={'Id': key})
        if 'Item' in response:
            print('Question paper already exists in the db. Fetching...')
            return response['Item']['question_paper']

    except Exception as e:
        print('Error while fetching initial dynamodb question paper',e)
        return {}

def extract_text(bucket_name, key):
    try:
        textract_job = textract_client.start_document_text_detection(
            DocumentLocation={
                'S3Object': {
                    'Bucket': bucket_name,
                    'Name': key
                }
            }
        )

        job_id = textract_job['JobId']

        print('Waiting for the extracion job to complete')
        while True:
            response = textract_client.get_document_text_detection(JobId=job_id)
            status = response['JobStatus']
            print(f"Job status: {status}")
            if status == 'SUCCEEDED':
                break
            elif status == 'FAILED':
                print('Job failed')
                return
            time.sleep(2)


        blocks = []
        next_token = None

        while True:
            if next_token:
                response = textract_client.get_document_text_detection(JobId=job_id, NextToken=next_token)
            else:
                response = textract_client.get_document_text_detection(JobId=job_id)

            blocks.extend(response.get('Blocks', []))

            next_token = response.get('NextToken', None)
            if not next_token:
                break
        
        
        lines = [block['Text'] for block in blocks if block['BlockType'] == 'LINE']
        extracted_text = '\n'.join(lines)

        return extracted_text

    
    except Exception as e:
        print('Error while extracting text from question paper', e)
        return None

def query_llm(prompt, text):

    full_prompt = (
        "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n"
        f"{prompt}\n\n {text}"
        "<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n"
        )
    input_body = {
        "prompt": full_prompt,
        "max_gen_len": 2000,
        "temperature": 0.2,
        "top_p": 0.8
    } 

    try:
        response = bedrock_client.invoke_model(
        modelId=model, 
        contentType="application/json",
        accept="application/json",
        body=json.dumps(input_body)
        )

        response_body = json.loads(response.get('body').read())
        print('llm response', response_body)

        generated_question_paper = response_body.get("generation", [{}])

        return generated_question_paper

    except Exception as e:
        print('Error while querying LLM...', e)
        return None

def evaluate_answersheet(bucket_name, key, question_paper_json):

    print("Evaluating answer sheet", bucket_name, key)

    """
    run text extract job for question paper
    """
    try:
        extracted_text = extract_text(bucket_name, key)
    except Exception as e:
        print('Error while extracting text from question paper', e)
        return


    """
    Fetch answersheet  prompt
    """
    try:
        with open('answersheet_evaluation_prompt.txt', 'r') as file:
            answersheet_prompt = file.read()

    except FileNotFoundError:
        print("Answersheet prompt text file not found.")
        return


    """
    Call gpt-oss-120B LLM for parsed question paper
    """
    evaluation_prompt = (f"{answersheet_prompt}. Here is the question paper json {question_paper_json}."
            f"Here is the extracted text {extracted_text}" )
    try:
        llm_response = query_llm(evaluation_prompt, extracted_text)

        print('Evaluation Response', llm_response)

    except Exception as e:
        print('Error while querying LLM...', e)
        return

    """
    Save the answer sheet evaluation to dynamodb
    """
    try:

        if not llm_response:
            print('No answer sheet text generated. Possible because of text might not be extracted properly')
            raise Exception('No answer sheet text generated. Possible because of text might not be extracted properly')

        table.put_item(
                Item={
                    'Id': key,
                    'question_paper': llm_response
                }
            )
        print('Question paper saved to dynamodb...')
        return llm_response
    except Exception as e:
        print('Error while saving question paper to dynamodb', e)
        return

