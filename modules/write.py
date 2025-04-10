from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, sha2, current_timestamp, col

def add_scd1_column(input_df:DataFrame, input_column:None) -> DataFrame:
    """
    Adds SCD1 columns to the input DataFrame.
    
    Parameters:
    input_df (DataFrame): The input DataFrame.
    input_column (None): The column to use for system timestamps. If None, current timestamp is used.
    
    Returns:
    DataFrame: The DataFrame with added SCD1 columns.
    """
    if input_column:
        input_df = input_df.withColumn("hash_key",sha2(concat_ws(*input_df.columns), 256))\
            .withColumn("system_created_at",col(input_column))\
            .withColumn("system_updated_at", col(input_column))
    else:
        input_df = input_df.withColumn("hash_key",sha2(concat_ws(*input_df.columns), 256))\
            .withColumn("system_created_at",current_timestamp())\
            .withColumn("system_updated_at",current_timestamp())

    return input_df

def identify_new_and_updated_data(input_df:DataFrame, target_table:DataFrame) -> DataFrame:
    """
    Identifies new and updated data by performing a left anti join with the target table.
    
    Parameters:
    input_df (DataFrame): The input DataFrame.
    target_table (DataFrame): The target table DataFrame.
    
    Returns:
    DataFrame: The DataFrame containing new and updated data.
    """
    new_and_updated_df = input_df.join(target_table, how="left_anti",on="hash_key")

    return new_and_updated_df

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