## This file shows examples to create a naive feature store by reading the data from AWS Redshift and writing it back to a table in again in AWS Redshift

import psycopg2
from dstk.utils import logger
import pandas as pd
import os,sys,arrow,runpy
import timeit
import concurrent.futures
import functools

def make_connection():
    """
    Examples
    ----
    >>> engine = make_connection()
    >>> query = "SELECT * from public.unified_data limit 10"
    >>> s = engine.cursor()
    >>> rr = s.execute(query)
    Returns
    -------
    session and engine for redshift.
    """

    from dstk.credstash_config_provider import CredstashConfigProvider
    config_provider = CredstashConfigProvider(
        kms_key_name='test',
        dynamodb_table='test',
        aws_region='test',
    )
    REDSHIFT_USERNAME = config_provider.get("redshift_username")
    REDSHIFT_PASSWORD = config_provider.get("redshift_password")
    REDSHIFT_DATABASE = config_provider.get("redshift_database")
    REDSHIFT_HOST = config_provider.get('redshift_host')
    REDSHIFT_PORT = config_provider.get('redshift_port')

    engine = psycopg2.connect(dbname=REDSHIFT_DATABASE, host=REDSHIFT_HOST, port=REDSHIFT_PORT, user=REDSHIFT_USERNAME, password=REDSHIFT_PASSWORD)
    logger.info(engine)

    return engine
    
  
  def read_query_for_feature_store(file_name) -> pd.DataFrame:
    """
    Reads an SQL File contained within one of the feature_helpers sql files and returns a dataframe.

    Reads the data in a way such that it will handle reading large files better.

    Parameters
    ----------
    file_name : SQL query containing the columns that need to be loaded in python
    """
    engine = make_connection()

    query = read_plain_sql(file_name)

    transaction = engine.cursor()
    try:
        transaction.execute(query)
        logger.info("Query Finished executing, loading into dataframe.")
        df = pd.DataFrame(transaction.fetchall())
        df.columns = [column[0] for column in transaction.description]
    finally:
        engine.commit()
        transaction.close()
        engine.close()
    logger.info("Loaded Query for Feature Store entry from {}".format(file_name))

    return df
    
    def create_table_if_not_exists(create_table, cursor, engine) -> None:
    """
    :param create_table:  query to create table in case the check_table does not exists
    :param cursor: Database cursor object
    :param engine: Database connection object
    :return: No return value
    """
    try:
        cursor.execute(create_table)
        # cursor.execute(create_temporary_feature_store_table)
        cursor.execute(create_feature_metadata_table)
        cursor.execute(create_feature_store_log_table)
    except Error as e:
        logger.info(e.pgerror)
        engine.rollback()
        raise e
        
def create_column_if_not_exists(engine,cursor,column,data_type = 'VARCHAR',schema = 'data_science', table = 'feature_store'):
    """
    :param engine: Database connection object
    :param cursor: Database connection object
    :param column: feature under consideration
    :param data_type: datatype for the feature that needs to be loaded
    :param schema: default schema of data_science
    :param table: default table of feature_store
    :return: None
    """
    cursor.execute(column_check.format(schema,table))
    columns = [column_name[0] for column_name in cursor]

    if column in columns:
        date_today = arrow.now().date().strftime('%Y_%m_%d')
        new_column = column + '_' + date_today

        update_query = update_column_values.format(column, new_column)
        table_query = add_feature_store_column.format(column, data_type)
        # temporary_table_query = add_feature_store_staging_column.format(column, data_type)

        try:
            cursor.execute(update_query)
            logger.info("Updated the historical {} to {}".format(column, new_column))

            cursor.execute(table_query)
            logger.info('Created new column {} in the table {}'.format(column, table, schema))
        except Error as e:
            logger.info(e.pgerror)
            engine.rollback()
            raise e
    else:

        table_query = add_feature_store_column.format(column, data_type)

        try:
            cursor.execute(table_query)
            logger.info('Created new column {} in the table {}'.format(column,table, schema))
        except Error as e:
            logger.info(e.pgerror)
            engine.rollback()
            raise e

def create_staging_table(engine,cursor,column,data_type = 'VARCHAR'):
    """

    :param engine: Database connection object
    :param cursor: Database connection object
    :param column: feature under consideration
    :param data_type: datatype for the feature that needs to be loaded
    :param schema: default schema of data_science
    :param table: default table of feature_store
    :return: None
    """

    temporary_table_query = add_feature_store_staging_column.format(column, data_type)
    try:
        cursor.execute(create_temporary_feature_store_table)
        cursor.execute(temporary_table_query)
    except Error as e:
        logger.info(e.pgerror)
        engine.rollback()
        raise e


def create_dependent_variable_if_not_exists(dependent_variables, schema ='data_science', table ='feature_store'):
    """
    TODO: this actually means create independent variables for a (derived) feature that depends on them
    :param cursor: Database connection object
    :param dependent_variable: feature under consideration
    :param data_type: datatype for the feature that needs to be loaded
    :param schema: default schema of data_science
    :param table: default table of feature_store
    :return: None
    """
    from database_interface.update_feature_store import create_executable_module
    engine = make_connection()
    cursor = engine.cursor()
    cursor.execute(column_check.format(schema, table))
    variables = [column_name[0] for column_name in cursor]

    for variable in dependent_variables:

        if variable in variables:
            logger.info("Checking if {feature} is in feature store".format(feature=variable))
            drop_column(variable)
            logger.warning('Removed previous version of independent variable. '
                           'Now updating independent variable: {feature}.'.format(feature=variable).upper())

        files = glob(os.path.join(REPO_PATH, 'feature_engineering', '*.py'))
        feature = pd.DataFrame(columns=['feature_name', 'function_name'])

        for file in files:
            if 'init' not in file and 'woe' not in file:  # to avoid making it executable

                modules = runpy.run_path(file, run_name="__main__")
                extract_df = pd.DataFrame(list(modules.items()), columns=['feature_name', 'function_name'])

                file_name = (file.split(".py"))[0]
                file_name = (file_name.split("/"))[5]

                extract_df['file_name'] = file_name
                feature = feature.append(extract_df)

        feature = feature.query('feature_name == @variable')
        feature['module_path'] = 'feature_engineering.' + feature.file_name

        module_xname = str(feature.iloc[0].module_path)
        xname = str(feature.iloc[0].feature_name)

        function = create_executable_module(module_xname,xname)
        logger.info("Adding {} to the feature store which is a dependent variable for the feature in consideration".format(function.__name__))
        function()


def write_metadata(feature_metadata,cursor, engine):
    """
    :param metadata_df: Metadata regarding the feature value
    :param connection: db cursor
    :return: None
    """
    metadata_df = pd.DataFrame(feature_metadata, index= [0])
    metadata_df = metadata_df[['feature_name','owner','version','date_first_create','is_tested','any_descriptive_stats','text_description']]
    data_upload = ','.join(cursor.mogrify('(%s,%s,%s,%s,%s,%s,%s)', row).decode('utf-8')  for row in metadata_df.values.tolist())

    try:
        cursor.execute('Insert INTO data_science.feature_metadata values ' + data_upload)
    except Error as e:
        engine.rollback()
        raise e
    else:
        logger.info("Successfully stored the feature metadata")
        
 def write_features(feature_df, feature_metadata, feature_name, column_data_type ='VARCHAR',
                   update_text_description_only=False):
    """
    :param feature_df: The feature values and name that needs to be uploaded to the feature store
    :param metadata_df: Metadata regarding the feature value
    :return: No return value
    Notes: For feature df , please keep the 'application_id' is the index
    """
    engine = make_connection()
    cursor = engine.cursor()

    try:
        if update_text_description_only:
            schema = 'data_science'
            table = 'feature_store'
            cursor.execute(column_check.format(schema, table))
            variables = [column_name[0] for column_name in cursor]
            if feature_name not in variables:
                create_dependent_variable_if_not_exists(dependent_variables=[feature_name])
            update_feature_text_description(feature_name, feature_metadata['text_description'])
            logger.info("ONLY writing TEXT DESCRIPTION for feature: {}".format(feature_name))
        else:
            df = feature_df.copy()
            assert len(df.columns.tolist()) == 2
            assert df.columns.values[1] != 'application_id'
            col = df.columns.values[1]
            df.loc[df[col].isnull(), col] = psycopg2.extensions.AsIs('NULL')
            df_length = len(df)

            create_table_if_not_exists(create_feature_store_table, cursor, engine)
            create_column_if_not_exists(engine, cursor, feature_name, column_data_type)
            # Batch insert values
            step_size = 200_000
            for i in range(0, df_length, step_size):

                # Push data to redshift in staging table
                create_staging_table(engine, cursor, feature_name, column_data_type)
                data_upload = ','.join(
                    cursor.mogrify('(%s,%s)', row).decode('utf-8') for row in df.iloc[i: i + step_size].values.tolist())
                cursor.execute('Insert INTO feature_store_staging values ' + data_upload)

                # update
                update_value = update_feature_store_from_staging.format(feature_name, feature_name)
                cursor.execute(update_value)
                logger.info("{} Records Updated".format(cursor.rowcount))

                # insert
                insert_value = insert_feature_store_from_staging.format(feature_name)
                cursor.execute(insert_value)
                logger.info("{} Records Inserted".format(cursor.rowcount))

                cursor.execute(drop_temporary_table)

            cursor.execute(meta_data_column_check.format(feature_metadata['feature_name'], feature_metadata['owner'],
                                                         feature_metadata['version'],
                                                         feature_metadata['date_first_create']))
            result = cursor.fetchone()[0]

            if not result:
                write_metadata(feature_metadata, cursor, engine)
            else:
                # Only update text_description if metadata already exists
                update_feature_text_description(feature_name, feature_metadata['text_description'])

    except Error as e:
        logger.info(e.pgerror)
        engine.rollback()
        raise e
    else:
        engine.commit()
        logger.info("Successfully stored {} to feature store".format(feature_name))
        cursor.close()
    finally:
        engine.close()

    grant_permissios_to_team()


def drop_column(column_name):
    """
    Delete a column from both the feature store and the meta-data.

    """
    engine = make_connection()
    cursor = engine.cursor()

    try:
        delete_column = "ALTER table data_science.feature_store drop column {}".format(column_name)
        cursor.execute(delete_column)
        logger.info("Successfully Deleted {}".format(column_name))
        delete_metadata = """DELETE FROM data_science.feature_metadata
                          WHERE feature_name = '{}'""".format(column_name)
        cursor.execute(delete_metadata)
        logger.info("Successfully Deleted {} from MetaData".format(column_name))
    except Error as e:
        logger.info(e.pgerror)
        engine.rollback()
        raise e
    finally:
        engine.commit()
        cursor.close()
        


    
    
    
