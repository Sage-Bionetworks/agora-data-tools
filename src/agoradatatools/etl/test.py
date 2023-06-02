import pandas as pd
import json

def describe_dataset(df: pd.DataFrame) -> str:
    '''
     place holder for a function to describe datasets. We'll add tests and filter results from describe
    '''
    try:
        result = json.loads(df.head().to_json(orient='records'))
    except AttributeError:
        result = None

    return result