from ProtoPlan import TextInputFormat
from ProtoPlan import ValueType
from ProtoPlan import StratosphereExecutor
from ProtoPlan import Mapper
import sys
import re

# For debugging
f = open("pythonPlanOut.txt", "a")
def log(s):
    f.write(str(s) + "\n")
sys.stderr = open("pythonPlanError.txt", "a")

inputPath1 = r"file:///home/filip/Documents/stratosphere/hamlet10.txt"
inputPath2 = r"file:///home/filip/Documents/stratosphere/hamlet_small100-200.txt"
outputPath = r"file:///home/filip/Documents/stratosphere/resultPlan.txt"

def split(record, collector):
    filteredLine = re.sub(r"\W+", " ", record[0].lower()) 
    [collector.collect((s, 1)) for s in filteredLine.split()]
        
def count(iter, collector):
    sum = 0
    record = None
    
    for val in iter:
        record = val
        sum += 1
       
    if(record != None):
        collector.collect((record[0], int(sum)))

in1 = TextInputFormat(inputPath1)
in2 = TextInputFormat(inputPath2)

Mapper([in1, in2], split, [ValueType.String, ValueType.Int]) \
    .reduce(count, [ValueType.String, ValueType.Int]).key(0) \
    .outputCSV(outputPath, [0,1]).fieldDelimiter(",")  \
    .execute()
    
""" Longer version
input = TextInputFormat(inputPath)
mapper = input.map(split, [ValueType.String])
reducer = mapper.reduce(count, [ValueType.String, ValueType.Int])
#reducer.keyValue(ValueType.String, 0) \
out = reducer.outputCSV(outputPath, [ValueType.String, ValueType.Int])
out.execute()
"""