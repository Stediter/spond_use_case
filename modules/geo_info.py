from geopy.geocoders import Nominatim
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, concat, lit, to_timestamp, udf
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

def enrich_geography_by_coordinates(inputs_df: DataFrame) -> DataFrame:
    """
    Enriches a DataFrame with county and state information based on latitude and longitude.

    Args:
    inputs_df (DataFrame): Input DataFrame containing 'latitude' and 'longitude' columns.

    Returns:
    DataFrame: DataFrame enriched with 'county' and 'state' columns.
    """
    to_be_enriched_df = inputs_df.where("latitude is not null")
    skip_df = inputs_df.where("latitude is  null")

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