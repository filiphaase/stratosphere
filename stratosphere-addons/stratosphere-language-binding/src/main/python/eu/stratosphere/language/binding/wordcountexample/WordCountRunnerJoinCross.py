from ProtoPlan import TextInputFormat
from ProtoPlan import ValueType
import sys
import re

# For debugging
f = open("pythonPlanOut.txt", "w")
def log(s):
    f.write(str(s) + "\n")
sys.stderr = open("pythonPlanError.txt", "w")

inputPath = r"file:///home/filip/Documents/stratosphere/hamlet1000.txt"
alphabetPath = r"file:///home/filip/Documents/stratosphere/alphabet.txt"
outputPath = r"file:///home/filip/Documents/stratosphere/resultPlan.txt"

def dumbCross(rec1, rec2, collector):
    if (ord(rec1[0])-ord(rec2[0]) > 5):
        collector.collect(( str(rec1[0]) , (str(rec1[0])+str(rec2[0])) ))
    
def dumbJoin(rec1, rec2, collector):
    collector.collect( (str(rec1[0])+str(rec2[1]), 1) )

def split(record, collector):
    log("Split record: " + str(record))
    filteredLine = re.sub(r"\W+", " ", record[0].lower()) 
    [collector.collect((s,1)) for s in filteredLine.split()]
        
def count(iter, collector):
    sum = 0
    record = None
    
    for val in iter:
        record = val
        sum += 1
        
    if(record != None):
        collector.collect((record[0], int(sum)))

#input1 = TextInputFormat(inputPath)
input2 = TextInputFormat(alphabetPath)
input3 = TextInputFormat(alphabetPath)
input4 = TextInputFormat(alphabetPath)

log("setting up plan now")

#input2.map(split, [ValueType.String, ValueType.Int]) \
   # .cross(input4, dumbJoin, [ValueType.String]) \
input2.cross(input3, dumbCross, [ValueType.String, ValueType.String]) \
    .join(input4, dumbJoin, [ValueType.String, ValueType.String]) \
    .reduce(count, [ValueType.String, ValueType.Int]) \
    .outputCSV(outputPath) \
    .execute()
    
""" Longer version
input = TextInputFormat(inputPath)
mapper = input.map(split, [ValueType.String])
reducer = mapper.reduce(count, [ValueType.String, ValueType.Int])
#reducer.keyValue(ValueType.String, 0) \
out = reducer.outputCSV(outputPath, [ValueType.String, ValueType.Int])
out.execute()
"""