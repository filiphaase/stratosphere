from ProtoUtils import *
import sys
import os

""" 
The current python implementation for streaming data into a map-operator. 
"""
class ProtoMapper(object):
    
    def __init__(self, connection):
        self.f = open('pythonMapperOut.txt', 'w')
        self.__iter = Iterator(self.f, connection)
        self.__collector = Collector(self.f, connection)
    
    def log(self, s):
        self.f.write(str(s) + "\n")
    
    """ 
    As long java hasn't send a signal that all map-functions are done(and hasMore() returns False
    always read a single record and call the mapfunction with it
    """    
    def map(self, mapFunc):
        while(self.__iter.hasMore()):
            record = self.__iter.readSingleRecord()
            if record != None:
                mapFunc(record, self.__collector)
            self.__collector.finish()
    