from pyspark.sql.functions import col, date_format

def create_integer_datekeys(input_df, input_column_list):
    for column in input_column_list:
        input_df = input_df.withColumn(column + "_datekey", 
                                       date_format(col(column), 'yyyyMMdd').cast('int'))
    return input_df
