from ProtoUtils import *
import sys
import os

class Reducer(object):
    
    def __init__(self, conn):
        self.__connectionType = conn
        self.f = open('pythonReducerOut.txt', 'w')

        self.__iter = Iterator(self.f, conn)
        self.__collector = Collector(self.f, conn)
    
    def log(self, s):
        self.f.write(str(s) + "\n")
        
    def reduce(self, reduceFunc):
        while(self.__iter.hasMore()):
            reduceFunc(self.__iter, self.__collector)
            self.__collector.finish()
    