from ProtoUtils import *
import sys
import os

""" 
    The current python implementation for streaming data into a map-operator. 
"""
class ProtoReducer(object):
    
    def __init__(self, connection):
        self.f = open('pythonReducerOut.txt', 'w')
        self.__iter = Iterator(self.f, connection)
        self.__collector = Collector(self.f, connection)
    
    def log(self, s):
        self.f.write(str(s) + "\n")
        
    def reduce(self, reduceFunc):
        while(self.__iter.hasMore()):
            reduceFunc(self.__iter, self.__collector)
            self.__collector.finish()
    