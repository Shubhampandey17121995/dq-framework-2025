import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from common.constants import VAR_ERROR_RECORD_PATH
from datetime import datetime

def save_error_records(error_record_df,entity_id):
    """
    Save the error records to a location.

    args:
    error_record_df: dataframe containing records failed for a rule.
    entity_id: to store the datframe at entity level at path location.

    Steps:
        1. Create the path to store the df
        2. write the df at s3 error records path partioned as Y/M/D/entity_id.
    Output:
    path: a variable containing the path where error records are stored
    """
    try:
        path = f"{VAR_ERROR_RECORD_PATH}{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/{entity_id}"
        error_record_df.write.mode('append').format('').save(path)
    except Exception as e:
        print(e)


"""
entity_id = 10
path = f"{VAR_ERROR_RECORD_PATH}{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/{entity_id}"
print(path)
"""