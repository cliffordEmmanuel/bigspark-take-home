from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os
import sys

from base import data_5g_params, basic_schema

DATA_SOURCE = '/home/clifford/side/projects/bigspark/data'

USERNAME=data_5g_params['user']
PASSWORD=data_5g_params['password']
DATABASE=data_5g_params['database']

# had to do this because pyspark worker and driver 
# was pointing to different python versions
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# setting up spark session...
conf = SparkConf().setAppName("EL Spark App") \
                  .setMaster("local[*]")
sc = SparkContext(conf=conf)
spark = SparkSession.builder \
                    .config(conf=conf) \
                    .config("spark.jars", "postgresql-42.5.1.jar") \
                    .getOrCreate()


def read_data_from_flat_file(filename,schema, separator='|'):
    """Reads data from a flat file into a spark dataframe

    Args:
        filename (_type_): flat file
        schema (_type_): contains the description of the columns 
        and the associated datatypes
        separator (str, optional): _description_. Defaults to '|'.

    Returns:
        _type_: a spark dataframe containing the extracted data
    """
    filepath=f"{DATA_SOURCE}/{filename}"
    spark_df = spark.read.csv(filepath,sep=separator,schema=schema)

    return spark_df


def to_local_postgres(spark_df,table, mode='overwrite'):
    """Saves the content of spark dataframe into a postgres database table

    Args:
        spark_df (_type_): spark dataframe containing the dataset to be processed
        table (_type_): table name in the database
        mode (str, optional): specifies whether data in the underlying table should 
                    be overwritten appended to. Defaults to 'overwrite'.
    """
    spark_df.write.format("jdbc").options(
        url=f'jdbc:postgresql://localhost:5432/{DATABASE}',
        driver='org.postgresql.Driver',
        user=USERNAME,
        password=PASSWORD,
        dbtable=table,
    ).save(mode=mode)

def main():
    # read all sources
    customers = read_data_from_flat_file(basic_schema['customer']['filename'],basic_schema['customer']['schema'])
    customer_address = read_data_from_flat_file(basic_schema['customer_address']['filename'],basic_schema['customer_address']['schema'])
    customer_demographics = read_data_from_flat_file(basic_schema['customer_demographics']['filename'],basic_schema['customer_demographics']['schema'])


    # Writing data to local db
    to_local_postgres(customers,"customers")
    to_local_postgres(customer_address,"customer_address")
    to_local_postgres(customer_demographics,"customer_demographics")
    



if __name__ == "__main__":
    main()