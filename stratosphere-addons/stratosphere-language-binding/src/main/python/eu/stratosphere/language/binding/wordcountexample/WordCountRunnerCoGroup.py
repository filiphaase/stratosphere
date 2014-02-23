from ProtoPlan import TextInputFormat
from ProtoPlan import ValueType
import sys
import re

# For debugging
f = open("pythonPlanOut.txt", "w")

def log(s, file= f):
    file.write(str(s) + "\n")
sys.stderr = open("pythonPlanError.txt", "w")

inputTable = r"file:///home/filip/Documents/stratosphere/table1.txt"
inputAlphabet = r"file:///home/filip/Documents/stratosphere/alphabetNtoN.txt"
outputPath = r"file:///home/filip/Documents/stratosphere/resultPlan.txt"

def splitAlphabet(record, collector):
    filteredLine = re.sub(r"\W+", " ", record[0].lower()) 
    for s in filteredLine.split():
        collector.collect((s, 100))
        
def splitTable(record, collector):
    log("Split table record: " + str(record))
    filteredLine = re.sub(r"\W+", " ", record[0].lower()).split() 
    collector.collect((filteredLine[0], int(filteredLine[1])))
            
def coGroup(iter1, iter2, collector):
    s = ""
    sum = 0
    for i in iter1:
        s += str(i[0]) + "1:"
        sum += i[1]
        
    for j in iter2:
        s += str(j[0]) + "2:" 
        sum += j[1]
        
    if s != "":
        collector.collect((s, sum))       
            

alph = TextInputFormat(inputAlphabet).map(splitAlphabet, [ValueType.String, ValueType.Int])
table = TextInputFormat(inputTable).map(splitTable, [ValueType.String, ValueType.Int])     

table.coGroup(alph, coGroup, [ValueType.String, ValueType.Int]) \
    .outputCSV(outputPath) \
    .execute()
