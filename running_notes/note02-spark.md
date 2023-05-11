
Introduction to Apache Spark 
=============================

- General Purpose 
- In memory 
- Compute Engine/Data Processing Engine 


Compute Engine
==============
Storage             - HDFS, S3, Local Storage 
Resource Manager    - YARN, K8S, Mesos, ... 
Compute             - Spark (in place of MR) 

Plug and play compute engine which needs some Storage System and Resource Manager 

In memory
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



























Dataset (samples)
=================
1,2013-07-25 00:00:00.0,11599,CLOSED
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
3,2013-07-25 00:00:00.0,12111,COMPLETE
4,2013-07-25 00:00:00.0,8827,CLOSED
5,2013-07-25 00:00:00.0,11318,COMPLETE

Findout the no. of orders based on different status 

CLOSED, 100
PENDING_PAYMENT, 300
COMPLETE, 50

===============

(<key>, <value>)

CLOSED, 1
PENDING_PAYMENT, 1
COMPLETE, 1
CLOSED, 1

MAP transformation 


('CLOSED', 1),
('PENDING_PAYMENT', 1),
('COMPLETE', 1),
('CLOSED', 1),
('COMPLETE', 1),
('COMPLETE', 1),
('COMPLETE', 1)




10
5
6


Map transformation 
-------------------
1000 Input Records =====> 1000 Output Records 

ReduceByKey
------------
1000 Input Records =====> <= No. of input records (No. of unique Keys) 


    K ,  V
[('256', 10),
 ('12111', 6),
 ('11318', 6),
 ('7130', 7),
 ('2911', 6),
 ('5657', 12),
 ('9149', 4),
 ('9842', 7),
 ('7276', 5),
 ('9488', 7),
 ('2711', 3),
 ('333', 6),
 ('656', 5),
 ('6983', 6),
 ('4189', 3),
 ('4840', 2),
 ('5863', 6),
 ('8214', 5),
 ('7776', 8),
 ('1549', 4)]







JOB 
====
Its same as No. of Actions in our application 
if we have 10 Action ===> 10 Jobs
            1 Action ===> 1 Job 


Types of Transformation 
-----------------------
    1. Narrow Transformation 
        - map, flatmap, 

    w1      w2      w3

    p1      p2      p3

    o1      o2      o3

    2. Wide Transformation 

    reducebykey() 

    machine 1           machine 2           machine 3
    w1                  w2                  w3 

    (a,1)               (a,1)               (b,1)
    (b,1)               (a,1)               (a,1)
                        (b,1)
                        (b,1)
                        (b,1)


    (a,1)                                   
    (a,2)       ===> (a, 4)                  (b,1)              
    (a,1)                                    (b,3)          ====> (b,5)
                                             (b,1)              




STAGE 
=====
No. of stages = No. of Wide Transformations + 1

TASK
====
No. of partition 
10 partition ===> 10 Tasks 

This a single compution unit performed on a single data partition 

=====
JOB (No. of Actions) 
 - STAGES (0, 1, 2, .... ) (No. of number of wide transformations + 1)
    - Within each stage 
        TASKs --> (No. of partitions)  




We want to chnage the no. of partitions 
========================================

1) When we would like to increase ?   ====> repartition() 

50 blocks (50 partitions)

200 node cluster 

At max we can use only 50 nodes (150 free nodes)

We can use 'repartition()' 


2) When we would like to decrease ? ====> Coalesce()

1000 partitions (blocks of data)

100 node cluster 
    - each node might 10 paritions (10 * 100)
    - 128 MB each partitions 
    - each machine is having 10 such partitions 

    128MB partition ====> filter ======> 2MB 

    - 10 partitions are having only 2MB each 

100 machines 
    - 10 partitions 
    - partition size is just 2MB 


We can use 'repartition()' or coalesce() 

4 nodes and 12 partition (RDD1)
>> rdd1.coalesce(4) 

n1    ===> p1, p2, p3 =======> new_part_1

n2    ===> p4, p5, p6 =======> new_part_2

n3    ===> p7, p8, p9 =======> new_part_3

n4    ===> p10, p11, p12 =======> new_part_4


The difference between reduceByKey() and groupByKey() 
======================================================


Orders (Big File)  ==> 8 partitions
==================
'11599', 'CLOSED'
'256', 'PENDING_PAYMENT'
'12111', 'COMPLETE'
'8827', 'CLOSED'


Customers (Small File)   ===> 2 partitions
======================
256,Richard,Hernandez,XXXXXXXXX,XXXXXXXXX,6303 Heather Plaza,Brownsville,TX,78521
222,Mary,Barrett,XXXXXXXXX,XXXXXXXXX,9526 Noble Embers Ridge,Littleton,CO,80126
8827,Ann,Smith,XXXXXXXXX,XXXXXXXXX,3422 Blue Pioneer Bend,Caguas,PR,00725
4222,Mary,Jones,XXXXXXXXX,XXXXXXXXX,8324 Little Common,San Marcos,CA,9206


            n1           n2              n3              n4
Orders:     p1, p2       p3, p4          p5, p6(256)     p7, p8
Customers:  p1, p2(256)         


JOIN ====>               (256) (256)     






Broadcast Variable
-------------------

Orders (Big File)  ==> 8 partitions
==================
'11599', 'CLOSED'
'256', 'PENDING_PAYMENT'
'12111', 'COMPLETE'
'8827', 'CLOSED'


Customers (Small File)   ===> Broadcast Variable 
======================
256,Richard,Hernandez,XXXXXXXXX,XXXXXXXXX,6303 Heather Plaza,Brownsville,TX,78521
222,Mary,Barrett,XXXXXXXXX,XXXXXXXXX,9526 Noble Embers Ridge,Littleton,CO,80126
8827,Ann,Smith,XXXXXXXXX,XXXXXXXXX,3422 Blue Pioneer Bend,Caguas,PR,00725
4222,Mary,Jones,XXXXXXXXX,XXXXXXXXX,8324 Little Common,San Marcos,CA,9206


            n1           n2              n3                 n4
Orders:     p1, p2       p3, p4          p5, p6(256)        p7, p8
Customers:  customer     customer        customer           customer   (Small file, 10MB)


