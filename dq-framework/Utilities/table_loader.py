# Load configuration table data
def config_loader(s3_table_path,entity_id):
        """
        load the dataframe from path and filter data for given entity id

        args:
        s3_table_path/table_name : file_path or name of the configuration table.
        entity_id : id for which the data will be fileterd

        Steps:
                1. read the dataframe of the given file_path or table name
                2. from dataframe filter records related to given entity id

        Output:
        dataframe : the dataframe with the data from given entity id
        """


# load actual entity data
def entity_data_loader(s3_table_path):
        """
        This function is to load the actual data of an entity.
        This loads the dataframe from given file_path or table_name.

        args:
        s3_table_path : file_path or table_name of the data table

        Steps:
                1. read the dataframe of the given file_path or table name
        Output:
        dataframe : dataframe with data of the entity
        """

#comment