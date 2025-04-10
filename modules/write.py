from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, sha2, current_timestamp, col, lit

def add_scd1_columns(input_df:DataFrame, input_column:None) -> DataFrame:
    """
    Adds SCD1 columns to the input DataFrame.
    
    Parameters:
    input_df (DataFrame): The input DataFrame.
    input_column (None): The column to use for system timestamps. If None, current timestamp is used.
    
    Returns:
    DataFrame: The DataFrame with added SCD1 columns.
    """
    input_df = input_df.withColumn("hash_key",sha2(concat_ws(*input_df.columns), 256))
    if input_column:
        input_df = input_df\
            .withColumn("system_created_at",col(input_column))\
            .withColumn("system_updated_at", col(input_column))
    else:
        input_df = input_df\
            .withColumn("system_created_at",current_timestamp())\
            .withColumn("system_updated_at",current_timestamp())

    return input_df


def add_scd2_columns(input_df:DataFrame, input_column:None) -> DataFrame:
    """
    Adds SCD2 columns to the input DataFrame.
    
    Parameters:
    input_df (DataFrame): The input DataFrame.
    input_column (None): The column to use for system timestamps. If None, current timestamp is used.
    
    Returns:
    DataFrame: The DataFrame with added SCD2 columns.
    """
    input_df = input_df.withColumn("hash_key",sha2(concat_ws(*input_df.columns), 256))\
                       .withColumn("system_is_active", lit(True))
    if input_column:
        input_df = input_df\
            .withColumn("system_valid_from",col(input_column))\
            .withColumn("system_valid_to", lit(None))
    else:
        input_df = input_df\
            .withColumn("system_valid_from",current_timestamp())\
            .withColumn("system_valid_to",lit(None))

    return input_df


def identify_new_and_updated_data(input_df:DataFrame, input_target_table:DataFrame) -> DataFrame:
    """
    Identifies new and updated data by performing a left anti join with the target table.
    
    Parameters:
    input_df (DataFrame): The input DataFrame.
    target_table (DataFrame): The target table DataFrame.
    
    Returns:
    DataFrame: The DataFrame containing new and updated data.
    """
    new_and_updated_df = input_df.join(input_target_table, how="left_anti",on="hash_key")

    return new_and_updated_df


def identify_lines_SCD2(input_df:DataFrame, input_target_table:DataFrame,input_key_list) -> (DataFrame,DataFrame):
    """
    Identifies new and updated lines for SCD2 processing.
    
    Parameters:
    input_df (DataFrame): The input DataFrame.
    input_target_table (DataFrame): The target table DataFrame.
    input_key_list (list): The list of key columns for the join condition.
    
    Returns:
    tuple: A tuple containing DataFrames for new and updated lines, and deactivated memberships.
    """
    new_and_updated_lines_df = input_df.join(input_target_table, how="left_anti",on="hash_key")
    deactivated_memberships_ids_df = input_target_table.where("system_is_active IS True").join(input_df, how="left_anti",on=input_key_list)

    return new_and_updated_lines_df, deactivated_memberships_ids_df


def merge_df_to_table(input_spark,
                      input_df:DataFrame,
                      input_target_table_reference:str,
                      input_update_columns_list:list,
                      input_key_columns_list:list):
    """
    Merges the input DataFrame into the target table using the specified key and update columns.
    
    Parameters:
    input_df (DataFrame): The input DataFrame.
    input_target_table_reference (str): The reference to the target table.
    input_update_columns_list (list): The list of columns to update.
    input_key_columns_list (list): The list of key columns for the join condition.
    """
    input_df.createOrReplaceTempView("df_to_merge")

    join_condition =  " AND ".join(["target."+ column + " = source." + column for column in input_key_columns_list])
    update_set = ",".join(["target."+ column + " = source." + column for column in input_update_columns_list])
    insert_columns = ",".join(input_df.columns)
    insert_columns_values = ",".join(["source." + column for column in input_df.columns])

    input_spark.sql(f"""  MERGE INTO {input_target_table_reference} AS target
                    USING df_to_merge AS source
                    ON {join_condition}
                    WHEN MATCHED THEN
                    UPDATE SET
                        {update_set}
                    WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_columns_values})""")
    

def deactivate_rows_SCD2(input_spark,
                         input_new_lines_df:DataFrame,
                         input_deactivated_df:DataFrame,
                         input_target_table_reference:str,
                         input_key_columns_list:list):
    """
    Deactivates rows in the target table for SCD2 processing.
    
    Parameters:
    input_new_lines_df (DataFrame): The DataFrame containing new lines.
    input_deactivated_df (DataFrame): The DataFrame containing deactivated lines.
    input_target_table_reference (str): The reference to the target table.
    input_key_columns_list (list): The list of key columns for the join condition.
    """
    input_deactivated_df = input_deactivated_df\
        .select(*input_key_columns_list)\
        .withColumn("system_valid_to", current_timestamp())\
        .withColumn("system_is_active", lit(False))

    input_new_lines_df = input_new_lines_df\
        .withColumn("system_valid_to", col("system_valid_from"))\
        .withColumn("system_is_active", lit(False))\
        .select(*input_key_columns_list,"system_valid_to","system_is_active")\

    lines_to_deactivate_df = input_deactivated_df.union(input_new_lines_df)

    lines_to_deactivate_df.createOrReplaceTempView("df_to_merge")

    join_condition =  " AND ".join(["target."+ column + " = source." + column for column in input_key_columns_list]) + " AND target.system_is_active = True"

    input_spark.sql(f"""  MERGE INTO {input_target_table_reference} AS target
                    USING df_to_merge AS source
                    ON {join_condition}
                    WHEN MATCHED THEN
                    UPDATE SET
                        target.system_valid_to = source.system_valid_to,
                        target.system_is_active = source.system_is_active""")
    

def append_df_to_table(input_df,input_target_table_reference):
    """
    Appends the input DataFrame to the target table.
    
    Parameters:
    input_df (DataFrame): The input DataFrame.
    input_target_table_reference (str): The reference to the target table.
    """
    input_df.write.mode("append").saveAsTable(input_target_table_reference)