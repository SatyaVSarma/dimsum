# Welcome
Hello, you've reached DIMSUM, a school project aimed at computing singular values of big matrices.

Main goal was to utilize distributed frameworks like Hadoop and Spark.

# Requirements
- Java 8
- Hadoop 2.7
- Spark 1.6.0

# Usage
In any case, please:
~~~
git clone https://github.com/antisrdy/dimsum
cd dimsum
~~~
## Hadoop implementation
This subsection tells the reader how to run the algorithm in a distributed mode from scratch:
- Generation of a large sparse matrix
- Computation of cosine similarities via Hadoop jobs (DIMSUM algorithm)
- Comparison with true values

To execute all this, please run:
~~~
python take_off.py m n
~~~
`m` and `n` being matrix shape.
## Spark test
