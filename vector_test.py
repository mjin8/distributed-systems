from app import vector_clock
import sys
import os
from flask import request
import requests

d1 = {0:0,1:0,2:0,3:0}
d2 = {0:0,1:1,2:0,3:0}
d3 = {0:0}
d4 = {0:0,1:0,2:1,3:0}
c1 = vector_clock(d1)
c2 = vector_clock(d2)
c3 = vector_clock(d3)
c4 = vector_clock(d4)
print ('c1', c1.get_clock().items())
print ('c2', c2.get_clock().items())
print ('c3', c3.get_clock().items())
print ("clock", c1.get_clock().items())
print ('c4', c4.get_clock().items())
new_clock = c1.increment(1)
print ("increment clock idx 1", new_clock.get_clock().items())
new_clock = c1.merge(c2,1)
print ("merge clocks", new_clock.get_clock().items())
print ('c1 is after c2', [c.get_clock().items() for c in c1.isafter([c2])])
print ('c2 is after c1', [c.get_clock().items() for c in c2.isafter([c1])])
print ('c3 is after c1', [c.get_clock().items() for c in c3.isafter([c1])])
print ('c1 is after c3', [c.get_clock().items() for c in c1.isafter([c3])])
print ('c1 is after c1', [c.get_clock().items() for c in c1.isafter([c1])])
print ('c1 is after c1, c2, c3', [c.get_clock().items() for c in c1.isafter([c1,c2,c3])])
print ('merge c1 and c2', c1.merge(c2,1))
