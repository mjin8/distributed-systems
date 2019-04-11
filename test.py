import math
import os
import sys

d1 = {}
d2 = {'hello': 'hi'}
d3 = {}
l = [1,2,3,4,5,6,7,8]
d3 = dict(list(d3.items()) + list(d2.items()))
print (d3)
print (len(d3.keys()))

d = {}
for n in range(4):
    d[n] = []
print (d)

n = 0
for i in l:
    if n == 4: n = 0
    d[n] += [i]
    n += 1
print (d)

l = [1,2,3,4,5,6,7]
old = 4
n = math.floor( len(l) / 2 )
for i in range (n-1, old):
    del d[i]

for n in range(3):
    d[n] = []
print (d)

n = 0
for i in l:
    if n == 3: n = 0
    d[n] += [i]
    n += 1
print (d)

for i in range (3, 5):
    print (i)
