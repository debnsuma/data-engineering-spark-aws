Introduction to Apache Spark 
=============================

- General Purpose 
- In momery 
- Compute Engine/Data Processing Engine 


Compute Engine
==============
Storage             - HDFS, S3, Local Storage 
Resource Manager    - YARN, K8S, Mesos, ... 
Compute             - Spark (in place of MR) 

Plug and play compute engine which needs some Storage System and Resource Manager 

In momery
==========
In MR 
======
            R               W               R               W                   R               W
Input Data ====> MR Job 1 ===> Output Data ====> MR Job 2 =====> Output Data =====> MR Job 3 ======> Output (Final)
                                 HDD/HDFS                           HDD/HDFS                            HDD/HDFS

                                 3 MR Jobs 
                                 No. of Disk IOs = 6 

In Spark 
=========

Read the data from HDFS/Storage =====> T1 ====> T2 ======> T3 ======> Final output to the disk/storage/HDFS 
                                            In the memory of the 
                                                worker nodes 
            3 Tranformation 
            No. of Disk IOs = 2 

General Purpose 
===============

MR ===> PIG (Cleaning) 
        HIVE (Querying)
        Mahoot (ML)
        Sqoop (Data Injection)

Spark ===> Querying, Cleaning, ML, .... 


--------

Spark has 2 Layers 
==================

Higher Level APIs 
    - Spark SQL 
    - DataFrames 
    - Streaming 
    - MLlib 
    - GraphX

Core APIs 
    - RDD (Resilient Distributed Dataset)
    - Different language 
        Python, Scala, R, Java 

RDD (Resilient Distributed Dataset)

No. of partition = No. of blocks 

What is Resilient here ?

In spark we will typically do these 3 things 

    1. We load some data 
    2. We will perform some Tranformations 
    3. We will save the processed data 

# Loading the data (#1)
rdd1 = load some file 

# These are transformations  (#2) <------ Transformations (Lazy)  
rdd2 = rdd1.map() 
rdd3 = rdd2.map()
rdd4 = rdd3.filter()

# Printing the processed data (#3)
rdd4.collect()                    <--------- Action 

Graph (DAG) -> Directed Acyclic Graph 
======================================







