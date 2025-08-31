import argparse
import os
import joblib
import pandas as pd
import xgboost as xgb
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-data-dir',
                        type=str,
                        default=os.environ['SM_OUTPUT_DATA_DIR'])
    parser.add_argument('--model-dir',
                        type=str,
                        default=os.environ['SM_MODEL_DIR'])
    parser.add_argument('--train',
                        type=str,
                        default=os.environ['SM_CHANNEL_TRAIN'])
    parser.add_argument('--val',
                        type=str,
                        default=os.environ['SM_CHANNEL_VAL'])

    args = parser.parse_args()

    train_df = pd.read_csv(os.path.join(args.train, 'train.csv'))
    val_df = pd.read_csv(os.path.join(args.val, 'val.csv'))

    print('Train top 5', train_df.head())

    X_train = train_df.drop('Gallstone Status', axis=1)
    y_train = train_df['Gallstone Status'].astype(int)

    X_val = val_df.drop('Gallstone Status', axis=1)
    y_val = val_df['Gallstone Status'].astype(int)

    scaler = StandardScaler()
    
    model = xgb.XGBClassifier(
        objective='binary:logistic',
        eval_metric='logloss',
        use_label_encoder=False,
        max_depth=4,
        random_state=42)

    model.fit(X_train, y_train,
              eval_set=[(X_val, y_val)],
              verbose=False)

    y_train_pred = model.predict(X_train)
    y_val_pred = model.predict(X_val)

    print("Train Accuracy: ", accuracy_score(y_train, y_train_pred))
    print("Validation Accuracy: ", accuracy_score(y_val, y_val_pred))

    # model.save_model(os.path.join(args.model_dir, 'xgboost-model.json'))
    joblib.dump(model, os.path.join(args.model_dir, 'model.joblib'))