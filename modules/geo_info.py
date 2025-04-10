from geopy.geocoders import Nominatim
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, concat, lit, to_timestamp, udf, date_format
from pyspark.sql import DataFrame

def get_county_and_state(coordinates):
    """
    Given a set of coordinates, returns the county and state.

    Args:
    coordinates (str): A string representing the latitude and longitude.

    Returns:
    dict: A dictionary with keys 'county' and 'state' containing the respective values.
    """
    geolocator = Nominatim(user_agent="diter")
    location = geolocator.reverse(coordinates)
    address = location.raw['address']

    return {"county": address["county"],"state":address["state"]}

get_county_and_state_udf = udf(get_county_and_state,
                               StructType([StructField('county', StringType(), True),
                                           StructField('state', StringType(), True)]))

def enrich_geography_by_coordinates(input_df: DataFrame, use_api:bool) -> DataFrame:
    """
    Enriches a DataFrame with county and state information based on latitude and longitude.

    Args:
    input_df (DataFrame): Input DataFrame containing 'latitude' and 'longitude' columns.

    Returns:
    DataFrame: DataFrame enriched with 'county' and 'state' columns.
    """
    if use_api:
        to_be_enriched_df = input_df.where("latitude is not null")
        skip_df = input_df.where("latitude is  null")

        to_be_enriched_df = to_be_enriched_df\
            .withColumn( "coordinates", concat(col("latitude"), lit(","), col("longitude")))\
            .withColumn("county_and_state", get_county_and_state_udf(col("coordinates")))\
            .withColumn("county", col("county_and_state").county)\
            .withColumn("state", col("county_and_state").state)\
            .drop(*["coordinates", "county_and_state"])
        
        skip_df = skip_df\
            .withColumn("county", lit(None))\
            .withColumn("state", lit(None))

        return skip_df.union(to_be_enriched_df)

    else:
        input_df = input_df\
            .withColumn("county", lit("Oslo county"))\
            .withColumn("state", lit("Norway"))
            
        return input_df
    