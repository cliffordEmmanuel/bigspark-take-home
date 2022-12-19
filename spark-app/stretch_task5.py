from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit, when
from pyspark.sql.types import DateType
import os
import sys


from base import batch_params, batch_schema
from functions.base import _get_batches_folders,_parse_contents,get_data_info


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


BATCH_DATA_SOURCE ='/home/clifford/side/projects/bigspark-take-home/data/tpcds_data_5g_batch'


# database configs

USERNAME=batch_params['user']
PASSWORD=batch_params['password']
DATABASE=batch_params['database']

BATCHES = _get_batches_folders(BATCH_DATA_SOURCE)
FILES_META_DATA = _parse_contents(BATCHES)


# setting up spark session...
conf = SparkConf().setAppName("Batch Spark App") \
                  .setMaster("local[*]")
sc = SparkContext(conf=conf)
spark = SparkSession.builder \
                    .config(conf=conf) \
                    .config("spark.jars", "postgresql-42.5.1.jar") \
                    .getOrCreate()


def read_data_from_flat_file(path,schema, separator='|'):
    """Reads data from a flat file into a spark dataframe

    Args:
        path (_type_): full path of the flat file
        schema (_type_): contains the description of the columns 
        and the associated datatypes
        separator (str, optional): _description_. Defaults to '|'.

    Returns:
        _type_: spark dataframe containing the extracted data
    """
    spark_df = spark.read.csv(path,sep=separator,schema=schema)
    return spark_df

def to_local_postgres(spark_df,table, mode='overwrite'):
    """Saves the content of spark dataframe into a postgres database table

    Args:
        spark_df (_type_): spark dataframe containing the dataset to be processed
        table (_type_): table name in the database
        mode (str, optional): specifies whether data in the underlying table should 
                    be overwritten appended to. Defaults to 'overwrite'.
    """
    print(f"Loading {spark_df.count()} records to {table} using {mode}!")

    spark_df.write.format("jdbc").options(
        url=f'jdbc:postgresql://localhost:5432/{DATABASE}',
        driver='org.postgresql.Driver',
        user=USERNAME,
        password=PASSWORD,
        dbtable=table,
    ).save(mode=mode)

def from_local_postgres(table):
    """reads the data from the database table into a spark dataframe

    Args:
        table (_type_): table name in the database

    Returns:
        _type_: spark dataframe containing the extracted data...
    """

    return spark.read.format("jdbc").options(
                url=f'jdbc:postgresql://localhost:5432/{DATABASE}',
                driver='org.postgresql.Driver',
                user=USERNAME,
                password=PASSWORD,
                dbtable=table,
            ).load()   
       
def batch_extract_and_load(batch:str, first_load:bool=False):
    """extract, transforms and loads all the files for a single batch load

    Args:
        batch (str): name of batch folder
        first_load (bool, optional): flag that shows whether this is the first data load. Defaults to False.
    """
    print("=======================================")
    print(f"Processing batch:{batch}")
    
    # getting the batched_timestamp
    # assuming that this is part of the folder name
    batched_at = batch.split('_')[1]

    # ===============================
    # for first load
    if first_load==True:
        print("first load..")
        # doing a separate one here for the first time load
        for data in get_data_info(batch,FILES_META_DATA):
            table=data[0]
            path=data[1]
            schema=batch_schema[table]['schema']
            load=batch_schema[table]['load']
            print(f"Processing data: {table}")
            # extract
            source_df = read_data_from_flat_file(path,schema)

            # transform
            source_df = source_df.withColumn('batched_at',lit(batched_at))
            source_df = source_df.withColumn('batched_at',source_df.batched_at.cast(DateType()))

            # load
            to_local_postgres(source_df,table)
            
            print(f"processing done for {table}")
    else:
        for data in get_data_info(batch,FILES_META_DATA):
            table=data[0]
            path=data[1]
            schema=batch_schema[table]['schema']
            load=batch_schema[table]['load']
            
            print(f"Processing data: {table} with {load}")

            # extract
            source_df = read_data_from_flat_file(path,schema)

            # transform and load
            if load=='truncate_load':
                # simulate a truncate and load: by doing an overwrite
                source_df = source_df.withColumn('batched_at',lit(batched_at))
                source_df = source_df.withColumn('batched_at',source_df.batched_at.cast(DateType()))
                to_local_postgres(source_df,table)
            elif load=='append':
                source_df = source_df.withColumn('batched_at',lit(batched_at))
                source_df = source_df.withColumn('batched_at',source_df.batched_at.cast(DateType()))
                to_local_postgres(source_df,table,mode='append')

            print(f"processing done for {table}")
       
    print(f"Done!! for {batch}")  
        
def is_tables_present():
    q="(SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public')) as tbl"
    result = spark.read.format("jdbc").options(url=f'jdbc:postgresql://localhost:5432/{DATABASE}',
                    driver='org.postgresql.Driver',dbtable=(q),user=USERNAME,
                    password=PASSWORD).load()

    result = result.select('exists').toPandas()['exists'][0]
    return result

def is_new_batch(batch):
    """checks if we have a new set of batch data to load.
    
    Assumes store sales is the fact and new data is always appended. 
    Not so proper I know!!
    """
    
    q="(select max(batched_at) as max_batch from store_sales ss) as tbl"
    result = spark.read.format("jdbc").options(url=f'jdbc:postgresql://localhost:5432/{DATABASE}',
                    driver='org.postgresql.Driver',dbtable=(q),user=USERNAME,
                    password=PASSWORD).load()

    result = result.select('max_batch').toPandas()['max_batch'][0]

    batched_at = batch.split('_')[1]
    
    if str(result) < batched_at:
        return True
    else:
        return False

def main():
    """starts here!!!
    """

    for batch in BATCHES:
        # checking if tables are present..
        present = is_tables_present()

        if present==False:
            print('tables not present, so first load')
            batch_extract_and_load(batch, first_load=True)
        else:
            # making sure we don't repeat for already loaded data...
            new_batch = is_new_batch(batch)
            if new_batch == True:
                # new batch found
                print("new batch found")
                batch_extract_and_load(batch, first_load=False)
            else:
                # no new batch do nothing
                print("batch already loaded do nothing")
        
        

if __name__ == "__main__":
    main()




