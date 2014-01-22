from ProtoUtils import *
import sys
import os

class Mapper(object):
    
    def __init__(self, conn):
        self.__connectionType = conn
        self.f = open('pythonMapperOut.txt', 'w')

        self.__iter = Iterator(self.f, conn)
        self.__collector = Collector(self.f, conn)
    
    def log(self, s):
        self.f.write(str(s) + "\n")
        
    def map(self, mapFunc):
        while(self.__iter.hasMore()):
            record = self.__iter.readSingleRecord()
            if record != None:
                mapFunc(record, self.__collector)
            self.__collector.finish()
    