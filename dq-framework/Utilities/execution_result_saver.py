from common.constants import VAR_S3_EXECUTION_RESULT_PATH


def save_results(result_data_dict):
        """
        Store the given result data at execution result table/location

        args:
        result_data_dict: a dictionary containing key:value pairs for the attributes in execution result table.

        Steps:
                1. create the df of result data dict.
                2. extract the entity id from result.
                3. create the path to store result.
                4. save the df at s3 location, partitioned by Y/M/D/entity_id
        """