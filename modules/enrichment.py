from pyspark.sql.functions import col, date_format

def create_integer_datekeys(input_df, input_column_list):
    """
    Generate integer date keys from date columns in a DataFrame.

    Args:
        input_df (DataFrame): The input Spark DataFrame containing date columns.
        input_column_list (list): List of column names (str) to be converted to integer date keys.

    Returns:
        DataFrame: The input DataFrame with additional columns containing integer date keys.
    """
    for column in input_column_list:
        input_df = input_df.withColumn(column + "_datekey", 
                                       date_format(col(column), 'yyyyMMdd').cast('int'))
    return input_df