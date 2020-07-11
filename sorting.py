
# coding: utf-8

# In[7]:


import heapq
from heapq import heappushpop, heappush, heappop


# In[3]:


class Solution:
    def kweakestRows(mat, k):
        return [x[1] for x in heapq.nsmallest(k, ((sums(s), i) for i, s in enumerate(mat)))]
        


# In[21]:


mat = [[2], [3, 5], [6, 7]]
k = 2
##make a generator with index and value
##pop smallest elements
heapq.nsmallest(k, ((sum(s), i) for i, s in enumerate(mat)))

