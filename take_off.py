# -*- coding: utf-8 -*-
#%%
import numpy as np
import subprocess
from scipy.linalg import norm
from sklearn.metrics.pairwise import cosine_similarity
import sys
#%%
def savetxt_dense(f_name, x, fmt="%.6g", delimiter=','):
    '''
    0 is well saved as 0 instead of 0.000... while saving dense matrices
    No loss of efficiency compared to np.savetxt as the original function
    loops over all rows. Rewriting the function is then the same
    '''
    with open(f_name, 'w') as fh:
        for row in x:
            line = delimiter.join("0" if value == 0 else fmt % value for value in row)
            fh.write(line + '\n')
    return True
#%%
clean_hadoop = 'hadoop fs -rmr /A.txt /B.txt /norms'
subprocess.call(clean_hadoop.split(' '))
print('----- [OK] Clean environment')
#%%
'''
Extract from paper
- Given an m × n matrix A with each row having at most L nonzero entries
- Entries of A have been scaled to be in [−1,1]
'''
##### Problem dimensioning
m = int(sys.argv[1]) # n_rows
n = int(sys.argv[2]) # n_cols
L = 3 # Sparsity constraint
print('--------------------------------------------------------------------')
print('----- Test on matrix size (%i, %i) with sparsity contraint L=%i'%(m, n, L))
print('--------------------------------------------------------------------')
#####

##### Generate A and save it
A = np.zeros((m, n)) # Row-based linked list sparse matrix
for row in range(A.shape[0]):
    n_cols = np.random.randint(0, L+1)
    if n_cols > 0:
        cols = np.random.choice(range(A.shape[1]), size=n_cols, replace=False)
        A[row, cols] = np.random.uniform(low=-1, high=1, size=n_cols)

print('----- [OK] Generation of A')
savetxt_dense('./data/A.txt', A)
print('----- [OK] Saving of A')
put_A = 'hadoop fs -put data/A.txt /'
subprocess.call(put_A.split(' '))
print('----- [OK] Upload of A to HDFS')

if len(sys.argv) == 4:
    naive = 'hadoop jar java.jar NaiveComputation /A.txt /naive'
    subprocess.call(naive.split(' '))
    print('----- [OK] Naive computation of A.TA')
    sys.exit()
#%%
'''
Extract from paper
In this case, the output of the reducers are random variables whose
expectations are cosine similarities i.e. normalized entries of AT A

Let's then define cosine similarity
'''

##### Cosine similarity of A
cosine_similarity_A = cosine_similarity(A.T)
print('----- [OK] Cosine similarity computation of A')
#%%
print('----- [Starting] Hadoop jobs')
norms = 'hadoop jar ./java/java.jar MatrixNorm /A.txt /norms'
subprocess.call(norms.split(' '))
print('----- [OK] Columns norms computation')
dimsum = 'hadoop jar ./java/java.jar DimSum /A.txt /B.txt'
subprocess.call(dimsum.split(' '))
print('----- [OK] Dimsum computation')
get_B = 'hadoop fs -getmerge /B.txt ./data/B.txt'
subprocess.call(get_B.split(' '))
print('----- [OK] Download of B')
#%%
B = np.zeros((n,n))
with open('./data/B.txt', 'r') as f:
    for line in f:
        line = line.split('\t')
        if len(line) > 0:
            B[int(line[0]), int(line[1])] = line[2]
#%%
##### Comapre distances
diff = norm(cosine_similarity_A - B) / norm(cosine_similarity_A)
print('----- RESULTS')
print('----- Difference between the true matrix and the DIMSUM one: %0.10f'%diff)