from Utils import *
import sys
import os

f = open('pythonReducerOut.txt', 'w')
def log(s):
    f.write(str(s) + "\n")

iter = Iterator(f)
collector = Collector(f)

# Program will be destroyed from java :D
# We could also handle this in the future by an exception from the iterator
while(True):
    sum = 0
    element = None
    for val in iter:
        element = val
        sum +=val[1]
    collector.collect((element[0], sum))