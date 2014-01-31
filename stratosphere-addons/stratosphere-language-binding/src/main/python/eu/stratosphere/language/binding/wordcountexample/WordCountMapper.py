from ProtoUtils import STDPipeConnection
from ProtoMapper import Mapper
import sys
import re

sys.stderr = open('pythonMapperError.txt', 'w')

def split(line, collector):
    filteredLine = re.sub(r"\W+", " ", line[0].lower()) 
    [collector.collect((s,1)) for s in filteredLine.split()]
        
connection = STDPipeConnection()
Mapper(connection).map(split)