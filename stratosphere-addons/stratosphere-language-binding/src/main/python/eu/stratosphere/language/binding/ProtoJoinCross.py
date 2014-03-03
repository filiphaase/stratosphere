from ProtoUtils import *
import sys
import os

""" 
    The current python implementation for streaming data into a cross or a join iterator.
    (For both the streaming is exactly the same: 2 recrods as input, one collector for the output  
"""
class ProtoJoinCross(object): 
    
    def __init__(self, connection):
        self.f = open('pythonJoinCrossOut.txt', 'w')
        self.__iter = Iterator(self.f, connection)
        self.__collector = Collector(self.f, connection)
    
    def log(self, s):
        self.f.write(str(s) + "\n")
        
    def join(self, joinFunc):
        while(self.__iter.hasMore()):
            record1 = self.__iter.readSingleRecord()
            if record1 != None:
                record2 = self.__iter.readSingleRecord()
                joinFunc(record1, record2, self.__collector)
            self.__collector.finish()
            
    def cross(self, crossFunc):
        self.join(crossFunc)
    