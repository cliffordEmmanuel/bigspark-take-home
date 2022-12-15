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


BATCH_DATA_SOURCE ='/home/clifford/side/projects/bigspark/data/tpcds_data_5g_batch'

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


def scd2_load(source_df,target_df,primary_key,batched_at,change_cols):
    """Implements a change data capture using scd type 2

    Args:
        source_df (_type_): contains the incoming dataset from source
        target_df (_type_): contains the target dataset from the destination db
        primary_key (_type_): the unique identifier of the dataset
        batched_at (_type_): time stamp showing when the data was batched, this is extracted from folder name
        change_cols (_type_): columns of the dataset where changes will be tracked

    Returns:
        _type_: a dataset with change tracking columns updated ie start_date, end_date
    """


    # let's use spark.sql for finding records that have changed.. based on the change columns...

    source_df.createOrReplaceTempView("source")
    target_df.createOrReplaceTempView("target")

    # generating a dynamic join statement
    joins = [f"(s.{x} == t.{x})" for x in change_cols]
    join_clause= " and ".join(joins)

    # looking for records that have changed between incoming and target data...
    unchanged_records = spark.sql(f"select t.{primary_key} from target t inner join source s on {join_clause};")

    # getting this into a list
    unchanged_records = list(unchanged_records.select(col(primary_key)).toPandas()[primary_key])

    # remove from source df records with ids in the unchanged_records
    new_source_df = source_df.filter(~col(primary_key).isin(unchanged_records))

    # new records 
    new_records = list(new_source_df.select(col(primary_key)).toPandas()[primary_key])

    # if no change is observed
    if len(new_records) ==0:
        print("no change observed!")
        return None
    else:
        # create the two scd columns in the new source_df
        # start date = current_batch date
        new_source_df = new_source_df.withColumn('start_date',lit(batched_at)).withColumn('end_date',lit(None))

        # type casting
        new_source_df = new_source_df.withColumn('start_date',new_source_df.start_date.cast(DateType()))\
                                        .withColumn('end_date',new_source_df.end_date.cast(DateType()))

        # now creating a new dataset waiting to be loaded..
        new_target_df = target_df.union(new_source_df)

        # now let's update the end_date in the target_df in normal run end_date current_batch date.
        updated_target_df = new_target_df.withColumn("end_date",when(col(primary_key).isin(new_records),lit(batched_at)).otherwise(lit(None)))

        return updated_target_df


def batch_extract_and_load(batch:str, first_load:bool=False):
    """extract, transforms and loads all the files for a single batch load

    Args:
        batch (str): name of batch folder
        first_load (bool, optional): flag that shows whether this is the first data load. Defaults to False.
    """

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
            print(f"Processing data: {table} with {load}")
            # extract
            source_df = read_data_from_flat_file(path,schema)

            # transform

            if load=='scd':
                # for scd tables we need a start_date and end_date
                source_df = source_df.withColumn('start_date',lit(batched_at))\
                                        .withColumn('end_date',lit(None))
                source_df = source_df.withColumn('start_date',source_df.start_date.cast(DateType()))\
                                        .withColumn('end_date',source_df.end_date.cast(DateType()))
            else:
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
            target_df = from_local_postgres(table)

            # transform and load
            if load=='scd':
                change_cols=batch_schema[table]['change_columns']
                primary_key=batch_schema[table]['primary_key']

                final_df = scd2_load(source_df, target_df, primary_key,batched_at,change_cols)
                # TODO: current limitation means entire dataset will have to be 
                # loaded and processed in memory for every new batch!!
                if final_df == None:
                    print("no change observed nothing to do here!!")
                else:
                    to_local_postgres(final_df,table)
            elif load=='truncate_load':
                # simulate a truncate and load: by doing an overwrite
                # indicating a batch load
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
        
        # making sure we don't repeat for already loaded data...
        new_batch = is_new_batch(batch)
        if new_batch == True:
            # new batch found
            print("new batch found")
            batch_extract_and_load(batch, first_load=False)
        else:
            # no new batch do nothing
            print("no new batch!! do nothing")
        
        

if __name__ == "__main__":
    main()




