# Code for Assessment 

This repo contains the code base for the following tasks attempted in the bigspark take [home assessment](https://github.com/itsbigspark/bigspark-challenge-spark#bigspark-data-engineering-challenge-spark).

The following tasks were attempted:
- [basic task] (https://github.com/itsbigspark/bigspark-challenge-spark#basic-task)
- [Data Engineering Core No. 3] (https://github.com/itsbigspark/bigspark-challenge-spark#optional-attempt-1-or-more-stretch-goals)
- [Data Engineering Core No. 5] (https://github.com/itsbigspark/bigspark-challenge-spark#optional-attempt-1-or-more-stretch-goals)


## Code Structure

The code structure is as follows:

### Airflow

In this folder, there are the dag scripts used to realize the orchestrating of the 2 data engineering core tasks.

### spark-app

In this folder are the spark jobs used.

### Zeppelin

In this folder are all the zeppelin notebooks used in process.


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


### TODO and limitations

I only uploaded 3 datasets and perform some basic visualization using the in built z.show visualization tool.