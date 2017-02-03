# Welcome
Hello, you've reached DIMSUM, a school project realized at ENSAE in February 2017.

End goal is to compute singular values of big matrices, inspired by http://stanford.edu/~rezab/papers/dimsum.pdf

For such a purpose, let's utilize distributed frameworks like **Hadoop** and **Spark**.

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
### Naive implementation
To run the naive computation of A.TA, please run:
~~~
python take_off.py m n naive
~~~
### DIMSUM implementation
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
