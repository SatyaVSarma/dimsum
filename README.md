# Welcome
Hello, you've reached DIMSUM, a school project realized at ENSAE in February 2017.

The objective is to compute singular values and similarities between columns of big matrices, inspired by http://stanford.edu/~rezab/papers/dimsum.pdf

For such a purpose, we will use distributed frameworks like **Hadoop** and **Spark**.

# Requirements
- Java 8
- Hadoop 2.7
- Spark 1.6.0

To run the following commands, it is assumed the reader has a functioning Hadoop/Spark cluster of machines.

# Usage
In any case, please:
~~~
git clone https://github.com/antisrdy/dimsum
cd dimsum
~~~
## Hadoop
### Naive implementation
To run the naive computation of A.TA (dot products), please run:
~~~
python take_off.py m n naive
~~~
`m` and `n` being the number of rows and columns respectively.
### DIMSUM implementation
This subsection explains how to run the algorithm in a distributed fashion, from scratch:
- Generation of a large sparse matrix
- Computation of cosine similarities via Hadoop jobs (DIMSUM algorithm)
- Comparison with true values

To execute all this, please run:
~~~
python take_off.py m n
~~~
## Spark
To run the implementation of DIMSUM in spark for computing column similarities, please run :
~~~
./spark/bin/spark-submit \
--master spark://master-ip \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir="/tmp" \
--packages "com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.1" \
--executor-memory 13g \
--driver-memory 13g  \
--class DIMSUM_ENSAE.Job ./spark/spark.jar
~~~
