from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def make_kpis(df):
    return df.groupBy('community_area', 'timestamp').agg(F.count('taxi_id').alias('amount of taxis'))


def filter_duplicates(df, subset):  
    return df.drop_duplicates(subset)


sc = SparkContext()
spark = SparkSession(sc)
spark.conf.set("temporaryGcsBucket", "taxi_data_temp")

sc.setLogLevel("ERROR")

taxi_data = spark.read.format("bigquery")\
                      .option("table", "bigquery-public-data:chicago_taxi_trips.taxi_trips")\
                      .load()

df_pickup = taxi_data.select('taxi_id', 
                             F.col('trip_start_timestamp').alias('timestamp'),
                             F.col('pickup_census_tract').alias('census_tract'),
                             F.col('pickup_community_area').alias('community_area'),
                             F.col('pickup_latitude').alias('latitude'), 
                             F.col('pickup_longitude').alias('longitude'),
                             F.to_date('trip_start_timestamp', 'yyyy-MM-dd').alias('date'),)
df_dropoff = taxi_data.select('taxi_id', 
                              F.col('trip_end_timestamp').alias('timestamp'),
                              F.col('pickup_census_tract').alias('census_tract'),
                              F.col('pickup_community_area').alias('community_area'),
                              F.col('dropoff_latitude').alias('latitude'), 
                              F.col('dropoff_longitude').alias('longitude'),
                              F.to_date('trip_end_timestamp', 'yyyy-MM-dd').alias('date'),)

taxi_location_df = df_pickup.unionByName(df_dropoff)

final_df = filter_duplicates(df=taxi_location_df, subset=['taxi_id', 'timestamp'])

kpi_df = make_kpis(final_df)

final_df.write.format("bigquery")\
              .option("table", "taxi_data.taxi_location")\
              .save()

kpi_df.write.format("bigquery")\
            .option("table", "taxi_data.taxi_kpis")\
            .save()
