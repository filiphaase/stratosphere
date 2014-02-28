from ProtoPlan import TextInputFormat
from ProtoPlan import ValueType
from ProtoPlan import StratosphereExecutor
import sys
import re

# For debugging
f = open("pythonPlanOut.txt", "a")
def log(s):
    f.write(str(s) + "\n")
sys.stderr = open("pythonPlanError.txt", "a")

inputPath = r"file:///home/filip/Documents/stratosphere/hamlet10.txt"
outputPath = r"file:///home/filip/Documents/stratosphere/resultPlan.txt"
outputPath2 = r"file:///home/filip/Documents/stratosphere/resultPlan2.txt"

def split(record, collector):
    filteredLine = re.sub(r"\W+", " ", record[0].lower()) 
    [collector.collect((1, s, 0.1, True)) for s in filteredLine.split()]
        
def count(iter, collector):
    sum = 0
    floatSum = 0.0
    record = None
    
    for val in iter:
        record = val
        sum += 1
        floatSum += val[2]
    
    bool = False
    if(int(sum) > 1):
        bool = True
        
    if(record != None):
        collector.collect((record[1], int(sum),floatSum, bool))

mapped = TextInputFormat(inputPath).map(split, [ValueType.Int, ValueType.String, ValueType.Float, ValueType.Boolean])
something = TextInputFormat(inputPath)
out = mapped.reduce(count, [ValueType.String, ValueType.Int, ValueType.Float, ValueType.Boolean]).key(1) \
    .outputCSV(outputPath, [0,1,2,3]).fieldDelimiter(",") 

out2 = mapped.reduce(count, [ValueType.String, ValueType.Int, ValueType.Float, ValueType.Boolean]).key(1) \
    .outputCSV(outputPath2, [0,1])

StratosphereExecutor.execute( [ out, out2 ] )
    
""" Longer version
input = TextInputFormat(inputPath)
mapper = input.map(split, [ValueType.String])
reducer = mapper.reduce(count, [ValueType.String, ValueType.Int])
#reducer.keyValue(ValueType.String, 0) \
out = reducer.outputCSV(outputPath, [ValueType.String, ValueType.Int])
out.execute()
"""