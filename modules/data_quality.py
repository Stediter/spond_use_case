from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit, to_timestamp

def check_data_quality_id(input_df:DataFrame, id_column:str) -> (DataFrame, DataFrame):
    """
    Checks the data quality of the ID column in the input DataFrame.
    
    Parameters:
    input_df (DataFrame): The input DataFrame to check.
    id_column (str): The name of the ID column to check.
    
    Returns:
    tuple: A tuple containing two DataFrames:
        - The first DataFrame contains rows with valid IDs.
        - The second DataFrame contains rows with null or duplicated IDs.
    """
    null_id_df = input_df.where(f"{id_column} IS NULL")
    input_df = input_df.where(f"{id_column} IS NOT NULL")

    if input_df.count() != input_df.select(id_column).distinct().count():
        duplicated_id_df = input_df.join(input_df.select(id_column).distinct(), on=id_column, how="left_anti")
        input_df = input_df.join(input_df.select(id_column).distinct(), on=id_column, how="inner")
        bad_id_df = duplicated_id_df.union(null_id_df)
    else:
        bad_id_df = null_id_df

    return input_df, bad_id_df


def check_data_quality_foreign_keys(input_df:DataFrame,check_list:list) -> (DataFrame,DataFrame):
    """
    Checks the data quality of foreign key columns in the input DataFrame.
    
    Parameters:
    input_df (DataFrame): The input DataFrame to check.
    check_list (list): A list of dictionaries, each containing:
        - "foreign_key_column" (str): The name of the foreign key column in the input DataFrame.
        - "cross_check_table" (DataFrame): The DataFrame to cross-check the foreign key against.
        - "cross_check_primary_key_column" (str): The primary key column in the cross-check DataFrame.
    
    Returns:
    tuple: A tuple containing two DataFrames:
        - The first DataFrame contains rows with valid foreign keys.
        - The second DataFrame contains rows with invalid foreign keys.
    """
    bad_formed_df = input_df.limit(0)
    for element in check_list:
        element["cross_check_table"] = element["cross_check_table"].withColumnRenamed(element["cross_check_primary_key_column"], element["foreign_key_column"]).select(element["foreign_key_column"])
        input_df = input_df.join(element["cross_check_table"], how="inner", on=element["foreign_key_column"])
        element_bad_formed = input_df.join(element["cross_check_table"], how="left_anti", on=element["foreign_key_column"])
        bad_formed_df = bad_formed_df.union(element_bad_formed)

    return input_df, bad_formed_df


def check_data_quality_timestamps(input_df:DataFrame, input_column_list:list, id_column:str) -> (DataFrame, DataFrame):
    """
    Checks the data quality of timestamp columns in the input DataFrame.
    
    Parameters:
    input_df (DataFrame): The input DataFrame to check.
    input_column_list (list): A list of column names to check for valid timestamps.
    id_column (str): The name of the ID column to use for joining.
    
    Returns:
    tuple: A tuple containing two DataFrames:
        - The first DataFrame contains rows with valid timestamps.
        - The second DataFrame contains rows with invalid timestamps.
    """
    for column in input_column_list:
        input_df = input_df.withColumn(column + "_check", to_timestamp(col(column)))

    check_condition = " OR ".join(column + "_check IS NULL" for column in input_column_list)
    bad_timestamp_df = input_df.where(check_condition)
    input_df = input_df.join(bad_timestamp_df, how="left_anti", on=id_column)
    for column in input_column_list:
        input_df = input_df.drop(column)
        input_df = input_df.withColumnRenamed(column + "_check", column)

    return input_df, bad_timestamp_df


def check_data_quality_events_table(input_df:DataFrame) -> (DataFrame, DataFrame):
    """
    Checks the data quality of the events table in the input DataFrame.
    
    Parameters:
    input_df (DataFrame): The input DataFrame to check.
    
    Returns:
    tuple: A tuple containing two DataFrames:
        - The first DataFrame contains rows with valid event data.
        - The second DataFrame contains rows with invalid event data.
    """
    bad_consecutio_df = input_df.where("event_start > event_end")
    input_df = input_df.where("event_start < event_end")

    bad_coordinates_df = input_df.where("(longitude is null and latitude is not null) OR (longitude is not null and latitude is null)")
    input_df = input_df.where("(longitude is null and latitude is null) OR (longitude is not null and latitude is not null)")

    bad_formed_df = bad_consecutio_df.union(bad_coordinates_df)

    return input_df, bad_formed_df