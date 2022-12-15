# Code for Assessment 

This repo contains the code base for the following tasks attempted in the bigspark take [home assessment](https://github.com/itsbigspark/bigspark-challenge-spark#bigspark-data-engineering-challenge-spark).

The following tasks were attempted:
- [basic task](https://github.com/itsbigspark/bigspark-challenge-spark#basic-task)
- [Data Engineering Core No. 3](https://github.com/itsbigspark/bigspark-challenge-spark#optional-attempt-1-or-more-stretch-goals)
- [Data Engineering Core No. 5](https://github.com/itsbigspark/bigspark-challenge-spark#optional-attempt-1-or-more-stretch-goals)


## Code Structure

The code structure is as follows:

### Airflow

In this folder, there are the dag scripts used to realize the orchestrating of the 2 data engineering core tasks.

### spark-app

In this folder are the spark jobs used.

# Tasks breakdown

## Basic Task

### Setup

This can be broken down into the following:
- Setup dev environment by installing spark and zeppelin notebook using the versions:
    - spark-3.1.3-bin-hadoop3.2
    - zeppelin-0.10.1-bin-all

- Env setup:
In the bashrc file add the variable defaults using the export command:
    - SPARK_HOME: _points to the spark installation folder_
    - JAVA_HOME: _points to java installation path_
    - PYSPARK_PYTHON: _points to where you have python installed ideally create a virtual environment and install python or use the default one in the **/usr/bin/python/**_
    - PYSPARK_DRIVER_PYTHON: _should be the same as pyspark python variable_

- Spark installation steps:
    - Download the spark binary.
    - Unpack into project folder: `tar -xzvf <name of tgz file>`
    - In the conf folder, 
        - rename the spark-env.sh.template to spark-env.sh and 
        - set the PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON paths

- Zeppelin installation steps:
    - Download the zeppelin binary
    - Unpack into project folder: `tar -xzvf <name of tgz file>`
    - In the zeppelin open conf folder:
        - rename the zeppelin-env.sh.template file (ie remove the .template extension)
        - set the java home, spark home, and pyspark home in the zeppelin-env.sh file
        - rename the zeppelin-site.xml.template file (ie remove the .template extension)
            - Update the port number: I used 8085. default is 8080.

- Starting the servers
    - To start the zeppelin server:
        - navigate to the zeppelin installation folder
        - ran cmd: `bin/zeppelin-daemon.sh start`

    - To stop the zeppelin server:
        - navigate to the zeppelin installation folder
        - ran cmd: `bin/zeppelin-daemon.sh stop`

    - To start the spark server:
        - navigate to the spar installation folder
        - ran cmd: `sbin/start-all.sh`

    - To stop the zeppelin server:
        - navigate to the zeppelin installation folder
        - ran cmd: `sbin/stop-all.sh`

    _use the cmd: `jps` to view the jvm services started_


### Task Steps

When the zeppelin server is started the UI will be rendered on the browser at localhost:8085 (using the port defined)
Setup the spark interpreter:
- Navigate to: Interpreter and search for spark
- Click on edit to update the interpreter properties:
    - SPARK_HOME: spark installation folder
    - spark.master: default local[*]
    - PYSPARK_PYTHON
    - PYSPARK_DRIVER_PYTHON

    _all of these should align with what was put in the conf/zeppelin-env.sh file_


### TODO and current limitations

I only uploaded 3 datasets and perform some basic visualization using the in built z.show visualization tool.

## Data Engineering Core No. 3

The task was to replicate the basic task and incorporate into a larger data processing pipeline. So i used postgres for a database and airflow for orchestrating the spark job.
### Setup

- A local installation of postgres.
- Downloaded a postgres jar file and uploaded to spark/jars folder.
- Local installation of airflow.
- Connected the zeppelin notebook to the postgres db.

### Task Steps

- Defined a data dictionary to store the schema (in the base.py script) for each of the flat files given,
- Created a pyspark script (spark-app/stretch_task3.py) that does the following:
    - Created a spark session specifying the postgres jar.
    - Includes functions that:
        - read from the dat files located in the data folder using `spark.read.csv` function
        - loads to the postgres db using the `spark.write.format('jdbc')`
- Created an dag script that orchestrates the entire process.
    - passed the above created pyspark script as a parameter to the SparkSubmitOperator.

### TODO and current limitations

- The airflow job was designed to be triggered manually.
- Only 3 datasets were considered and it was setup to be a one time load.
- Not a clear way to add a zeppelin notebook to the airflow pipeline 
so it can be automatically refreshed when new data is added.


## Data Engineering Core No. 5

The task was to redesign the problem as a batch scenario and simulate how it can be carried out.
### Setup

Used the same setup from the previous task.

### Task Steps

- Generated a meta_data dictionary:

The dataset was stored in batched folders, with the folder named having a timestemp added, which i assumed was the source extraction timestamp. I created three helper methods (_get_batches_folders,_parse_contents,get_data_info) to generate a data structure that contained meta data of the batch data, with this format:

```python
meta_data = {
    "<batch-folder-name>:(<table-name>,<full path to csv>)
}
```

- Created a data structure: batch_schema in the (base.py) to help with the batch loading of the data. 

The format is as follows:
```python
batch_schema = {
    "<table-name>":{
        "schema":"the schema for the dataset is defined here",
        "load":"specifies the loading strategy, could be either scd, append, truncate_load",
        "primary_key":"primary key of the dataset, used for the scd implementation",
        "change_columns":"for specifying the columns from the data set where change will be tracked, for the scd implementation",
    },
}
```

- Deciding on the loading strategy:

For all the datasets, the ff observations were made which informed the loading strategy:
1. 3 of the datasets didn't have any form of scd tracking for each batch namely:
    - customer
    - household_demographics
    - customer_address

    So Scd type 2 was implemented and 2 columns: *start_date*, and *end_date* were added to the table.

2. 5 tables however had a *rec_start_date* `rec_end_date* indicating that some kind of scd was already  done. So a truncate and load strategy was taken.
    - date_dim
    - time_dim
    - item
    - store
    - promotion

    So one column: *batched_at* was added to show the current batch loaded.

3. For the store_sales which is the fact table and append loading strategy was used.


- Created a pyspark script (spark-app/stretch_task5.py) that does the following:
    - Created a spark session specifying the postgres jar.
    - Includes functions that:
        - read from the dat files located in the data folder using `spark.read.csv` function
        - performs the specific loading strategy.

- Created an airflow dag script that orchestrates the entire process.
    - passed the above created pyspark script as a parameter to the SparkSubmitOperator.

### TODO and current limitations

- The airflow job was designed to be triggered manually.
- This design was highly tailored to having all the batched data present in the source folder, and may not generalized well when new batches are added. 
    - However some checks were implemented to ensure that:
        - if the database has no tables created the first batch is carried out as a full load, 
        with the required columns created for each load strategy created as well.
        - if the tables already exist:
            - check to see which current batch has been loaded, 
                - if the timestamp for the incoming batch is greater than the max batched_at loaded. 
                - confirm check and load the new batch
            - if no new batch, do nothing and just exit.
- The scd implementation, is resource heavy, and does the processing in memory, this may become an issue if the batch data grows, and more batches are added.