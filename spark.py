import io
import os
import requests
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, map_from_entries, col
from google.cloud import bigquery

# Create Spark session
spark = SparkSession.builder.appName("uber_etl_pipeline").getOrCreate()

def extract_data(*args, **kwargs):
    # Fetch the CSV data from the URL
    url = 'https://storage.googleapis.com/uber-data-engineering-project-darshil/uber_data.csv'
    response = requests.get(url)
    
    # Save the response content to a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
        temp_file.write(response.content)
        temp_file_path = temp_file.name
    
    # Load the CSV file into a PySpark DataFrame
    df = spark.read.csv(temp_file_path, header=True, inferSchema=True)

    # Clean up the temporary file
    os.remove(temp_file_path)
    
    return df



def transform_data(df, *args, **kwargs):
    
    # Convert columns to datetime
    df = df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))
    df = df.withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))

    # Create datetime dimension table
    datetime_dim = df.select("tpep_pickup_datetime", "tpep_dropoff_datetime").distinct()
    datetime_dim = datetime_dim.withColumn("pick_hour", col("tpep_pickup_datetime").hour)
    datetime_dim = datetime_dim.withColumn("pick_day", col("tpep_pickup_datetime").day)
    datetime_dim = datetime_dim.withColumn("pick_month", col("tpep_pickup_datetime").month)
    datetime_dim = datetime_dim.withColumn("pick_year", col("tpep_pickup_datetime").year)
    datetime_dim = datetime_dim.withColumn("pick_weekday", col("tpep_pickup_datetime").weekday())

    datetime_dim = datetime_dim.withColumn("drop_hour", col("tpep_dropoff_datetime").hour)
    datetime_dim = datetime_dim.withColumn("drop_day", col("tpep_dropoff_datetime").day)
    datetime_dim = datetime_dim.withColumn("drop_month", col("tpep_dropoff_datetime").month)
    datetime_dim = datetime_dim.withColumn("drop_year", col("tpep_dropoff_datetime").year)
    datetime_dim = datetime_dim.withColumn("drop_weekday", col("tpep_dropoff_datetime").weekday())

    datetime_dim = datetime_dim.withColumn("datetime_id", col("datetime_id").cast("int")) \
                          .setIndex("datetime_id")
    

    datetime_dim = df.select("tpep_pickup_datetime", "tpep_dropoff_datetime") \
                    .setIndex("datetime_id")  

    # create passenger_count dimension table
    passenger_count_dim = df.select("passenger_count").distinct() \
                            .setIndex("passenger_count_id") 

    # create trip_dist dimension table
    trip_distance_dim = df.select("trip_distance").distinct() \
                            .setIndex("trip_distance_id")  

    # create rate_code dimension table
    rate_code_type_map = lit(map_from_entries([
        (1, "Standard rate"), 
        (2, "JFK"), 
        (3, "Newark"), 
        (4, "Nassau or Westchester"),
        (5, "Negotiated fare"), 
        (6, "Group ride")
    ]))

    rate_code_dim = df.select("RatecodeID").distinct() \
                    .withColumn("rate_code_name", col("RatecodeID").cast("int").map(rate_code_type_map)) \
                    .setIndex("rate_code_id")  

    # create pickup_location dimension table
    pickup_location_dim = df.select("pickup_longitude", "pickup_latitude").distinct() \
                        .setIndex("pickup_location_id")  

    # create dropoff_location dimension table
    dropoff_location_dim = df.select("dropoff_longitude", "dropoff_latitude").distinct() \
                        .setIndex("dropoff_location_id")

    # create payment_type dimension table
    payment_type_name_map = lit(map_from_entries([
        (1, "Credit card"), 
        (2, "Cash"), 
        (3, "No charge"), 
        (4, "Dispute"),
        (5, "Unknown"),
        (6, "Voided trip")
    ]))

    payment_type_dim = df.select("payment_type").distinct() \
                    .withColumn("payment_type_name", col("payment_type").cast("int").map(payment_type_name_map)) \
                    .setIndex("payment_type_id") 
    
    # Fact table creation
    fact_table = df.join(passenger_count_dim, on="passenger_count") \
                .join(trip_distance_dim, on="trip_distance") \
                .join(rate_code_dim, on="RatecodeID") \
                .join(pickup_location_dim, on=["pickup_longitude", "pickup_latitude"]) \
                .join(dropoff_location_dim, on=["dropoff_longitude", "dropoff_latitude"]) \
                .join(datetime_dim, on=["tpep_pickup_datetime", "tpep_dropoff_datetime"]) \
                .join(payment_type_dim, on="payment_type") \
                .select("VendorID", "datetime_id", "passenger_count_id",
                        "trip_distance_id", "rate_code_id", "store_and_fwd_flag",
                        "pickup_location_id", "dropoff_location_id",
                        "payment_type_id", "fare_amount", "extra", "mta_tax",
                        "tip_amount", "tolls_amount", "improvement_surcharge",
                        "total_amount")
    
   # Write transformed data to Parquet
    datetime_dim_path = "datetime_dim.parquet"
    datetime_dim.write.mode("overwrite").parquet(datetime_dim_path)

    passenger_count_dim_path = "passenger_count_dim.parquet"
    passenger_count_dim.write.mode("overwrite").parquet(passenger_count_dim_path)

    trip_distance_dim_path = "trip_distance_dim.parquet"
    trip_distance_dim.write.mode("overwrite").parquet(trip_distance_dim_path)

    rate_code_dim_path = "rate_code_dim.parquet"
    rate_code_dim.write.mode("overwrite").parquet(rate_code_dim_path)

    pickup_location_dim_path = "pickup_location_dim.parquet"
    pickup_location_dim.write.mode("overwrite").parquet(pickup_location_dim_path)

    dropoff_location_dim_path = "dropoff_location_dim.parquet"
    dropoff_location_dim.write.mode("overwrite").parquet(dropoff_location_dim_path)

    payment_type_dim_path = "payment_type_dim.parquet"
    payment_type_dim.write.mode("overwrite").parquet(payment_type_dim_path)

    fact_table_path = "fact_table.parquet"
    fact_table.write.mode("overwrite").parquet(fact_table_path)

    # Returning dictionary of Parquet file paths
    return {
        "datetime_dim": datetime_dim_path,
        "passenger_count_dim": passenger_count_dim_path,
        "trip_distance_dim": trip_distance_dim_path,
        "rate_code_dim": rate_code_dim_path,
        "pickup_location_dim": pickup_location_dim_path,
        "dropoff_location_dim": dropoff_location_dim_path,
        "payment_type_dim": payment_type_dim_path,
        "fact_table": fact_table_path
    }


def load_data(data):
    # Initialize BigQuery client
    client = bigquery.Client()

    # Define your BigQuery dataset
    dataset_id = "ebube.uber_data_etl"

    # Load each Parquet file into its corresponding BigQuery table
    for table_name, file_path in data.items():
        # Load data from Parquet file into BigQuery table
        table_id = f"{dataset_id}.{table_name}"
        load_parquet_into_bigquery(client, file_path, table_id)


    def load_parquet_into_bigquery(client, file_path, table_id):
        # Load data from Parquet file into BigQuery table
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.PARQUET

        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(
                source_file, table_id, job_config=job_config
            )

        job.result()  # Wait

# Stop SparkSession
spark.stop()




