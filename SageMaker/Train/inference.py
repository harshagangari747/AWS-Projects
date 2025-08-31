import os
import json
import joblib
import pandas as pd


def model_fn(model_dir):
    print('listing files', os.listdir(model_dir))
    return joblib.load(os.path.join(model_dir,
                                    "model.joblib"))


def input_fn(request_body, request_content_type):
    if request_content_type == 'application/json':
        data = json.loads(request_body)
        if isinstance(data, dict):
            data = [data]
        return pd.DataFrame(data)
    else:
        raise ValueError("Unsupported type")


def predict_fn(input_data, model):
    predictions = model.predict(input_data)
    return predictions.tolist()


def output_fn(prediction, content_type):
    if content_type == 'application/json':
        return json.dumps({'predictions' : prediction})
    else:
        return ValueError("Unsupported content type: " + content_type)