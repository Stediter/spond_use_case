from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit, to_timestamp

# For all this test, with more time at my disposal I would have liked to explore DBX, a new framework from databricks which enables in a systematic way data quality control to a df splitting in a similar way to what I did manually

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
        bad_timestamp_df = bad_timestamp_df.drop(column + "_check")
        input_df = input_df.withColumnRenamed(column + "_check", column)

    return input_df, bad_timestamp_df


def check_data_quality_teams_table(input_df):
    """
    Checks the data quality of the teams table in the input DataFrame.
    
    Parameters:
    input_df (DataFrame): The input DataFrame to check.
    
    Returns:
    tuple: A tuple containing two DataFrames:
        - The first DataFrame contains rows with valid team data.
        - The second DataFrame contains rows with invalid team data.
    """
    bad_country_code_df = input_df.where("country_code IS NULL")
    input_df = input_df.where("country_code IS NOT NULL")

    bad_activity_df = input_df.where("team_activity IS NULL")
    input_df = input_df.where("team_activity IS NOT NULL")  

    bad_formed_df = bad_country_code_df.union(bad_activity_df)

    return input_df, bad_formed_df


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

    bad_coordinates_df = input_df.where("(longitude is null and latitude is not null) OR (longitude is not null and latitude is null) OR longitude > 180 or longitude < -180 or latitude > 90 or latitude < -90")
    input_df = input_df.where("(longitude is null and latitude is null) OR (longitude is not null and latitude is not null)")

    bad_formed_df = bad_consecutio_df.union(bad_coordinates_df)

    return input_df, bad_formed_df


def check_data_quality_memberships_table(input_df:DataFrame) -> (DataFrame, DataFrame):
    """
    Checks the data quality of the memberships table in the input DataFrame.
    
    Parameters:
    input_df (DataFrame): The input DataFrame to check.
    
    Returns:
    tuple: A tuple containing two DataFrames:
        - The first DataFrame contains rows with valid membership data.
        - The second DataFrame contains rows with invalid membership data.
    """
    wrong_role_df = input_df.where("role_title NOT IN ('member','admin')")
    input__df = input_df.where("role_title IN ('member','admin')")

    return input_df, wrong_role_df


def check_data_quality_events_rsvps_table(input_df:DataFrame,input_memberships_df:DataFrame, input_events_df:DataFrame) -> (DataFrame, DataFrame):
    """
    Checks the data quality of the events RSVPs table in the input DataFrame.
    
    Parameters:
    input_df (DataFrame): The input DataFrame to check.
    input_memberships_df (DataFrame): The DataFrame containing membership data.
    input_events_df (DataFrame): The DataFrame containing event data.
    
    Returns:
    tuple: A tuple containing two DataFrames:
        - The first DataFrame contains rows with valid RSVPs.
        - The second DataFrame contains rows with invalid RSVPs.
    """
    bad_response_df = input_df.where("rsvp_status < 0 OR rsvp_status >3")
    input_df = input_df.where("rsvp_status >= 0 AND rsvp_status <= 3")

    #This check does not consider the history of the event and membership table at the moment, that would require a more sofisticated control using join conditions for not only current tables but full history, not enough time for that
    input_memberships_df = input_memberships_df.withColumnRenamed("group_id","team_id")
    members_allowed_to_respond_to_events_df = input_memberships_df.join(input_events_df, how="inner", on="team_id").select("event_id","membership_id")

    rspvs_from_not_members = input_df.join(members_allowed_to_respond_to_events_df, how="left_anti",on=["event_id","membership_id"])
    input_df = input_df.join(members_allowed_to_respond_to_events_df, how="inner",on=["event_id","membership_id"])
    rspvs_from_not_members.schema

    bad_formed_df =  bad_response_df.union(rspvs_from_not_members)

    return input_df, bad_formed_df