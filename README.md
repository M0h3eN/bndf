# BNDF

## Introduction

BNDF is a library for storing and processing large-scale single(multi) unit and multichannel 
array recording data in a distributed manner. This library is build on top of [Apache Spark](https://spark.apache.org/) 
and [Apache Hadoop](https://hadoop.apache.org/). For storing large-scale raw data, [Apache Parquet](https://parquet.apache.org)
a columnar data structure and [Apache Hive](https://hive.apache.org/) are used on top of [Hadoop distributed file system (HDFS)](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html). 
Meta-data information of raw data, are constructed as nested [JSON](https://www.json.org/json-en.htm) files and stored in 
[mongoDB](https://www.mongodb.com). BNDF's APIs can be used in Scala, java, python, R and partially in Matlab.    

### Key Advantages

BNDF provide capabilities including, but not limited to:

* Scalable data processing.
* Efficient and fast data storage for experimenters.
* Efficient and fast data processing for data analyst. 
* A major movement toward standardized data and meta-data format.   

### File Format
Currently, BNDF supports MAT files as raw input data with conditions described in [MAT File Library](https://github.com/HebiRobotics/MFL). 

## Getting Started

### Building from Source

You will need to have [Apache Maven](https://maven.apache.org/) 3.6.0 or later installed in order to compile BNDF.

```bash
$ git clone https://github.com/M0h3eN/bndf.git
$ cd bndf
$ mvn install 
```

### Run and Deployment

BNDF could run on any cluster or single machine running and configured following tools

* [Apache Hadoop](https://hadoop.apache.org/)
* [Apache Spark](https://spark.apache.org/)
* [Apache Hive](https://hive.apache.org/)
* [Apache Zeppelin](https://zeppelin.apache.org/) (Optional)
* [mongoDB](https://www.mongodb.com)

BNDF executive jar file take two parameters in the following order 

* DATA_PATH
* MONGO_URI

```bash
$ spark-submit \ 
    --class com.ipm.nslab.bndf.${BNDF_MODULE_NAME} \
    --master ${SPARK_MASTER(s)_URL} | yarn | mesos \
    --deploy-mode client | cluster \ 
    --executor-memory ${SPARK_EXECUTOR_MEMORY}G \
    --total-executor-cores ${SPARK_EXECUTOR_CORES} \
    --driver-memory ${SPARK_DRIVER_MEMORY}G \
    PATH_TO_BNDF_JAR_FILE/bndf-${JAR_FILE_VERSION}.jar DATA_PATH  MONGO_URI
```

Spark-submit's parameters detailed information are available in [submitting-applications](https://spark.apache.org/docs/latest/submitting-applications.html).
For creating a private cluster and information about other runtime parameters not discussed here, see [BndfCluster](https://github.com/M0h3eN/bndfcluster.git).

### Data Locality

It is very important input data placed in the same network as cluster or, a fast access network that communicates with cluster's network. 

* Create a shared storage accessible by all the nodes in the cluster.
* Copy input data directly in HDFS using [WebHDFS REST API](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)
 or other protocol like [Mountable HDFS](https://docs.cloudera.com/documentation/enterprise/latest/topics/cdh_ig_hdfs_mountable.html).

Otherwise, data locality could create major bottlenecks while processing data with Spark.

## Limitation, Current, and Future work
Currently, BNDF is at its early stage of development and require various extensions to be a fully functional framework. These are the most well-known cases: 

- Currently, BNDF only supports MAT files. I am working on adding commonly used file format like, [Nwb](https://www.nwb.org), 
  [Nix](https://nixio.readthedocs.io/en/latest/getting_started.html), and [Nio](https://neo.readthedocs.io/en/stable/).
- The functionality of BNDF schema, or generally its parser is limited. Since most of the file formats which is commonly used in the neuroscience community are somehow based on the HDF5 file format,  I decided to create a general parser based on the HDF5 file format for converting commonly used file format into a more distributed friendly structure.
- From the processing perspective, BNDF currently has only distributed spike sorter module. Various extensions should be considered here, like:
  1. The Spike sorter module needs to be improved, and generalized in a way that it could handle more up-to-date and complex spike sorting algorithms.
  2. Different distributed processing algorithms should be added to BNDF.
- Currently, the usage of BNDF is restricted to a spark-submit job which is not trivial from the user's point of view. Several considerations may apply for ease of use in future works, for example:
  1. Creation of a shell-based application for interacting easily with BNDF.
  2. Creation of a web-based application to access BNDF core functionality even more easilly.


## Documentation

BNDF documentation are available in [bndf-doc](https://bndf.readthedocs.io/).