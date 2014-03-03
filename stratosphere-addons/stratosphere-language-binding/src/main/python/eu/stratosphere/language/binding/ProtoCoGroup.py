from ProtoUtils import *
import sys
import os

""" 
    The current python implementation for streaming data into a  
    coGroup Operator
"""
class ProtoCoGroup(object): 
    
    def __init__(self, connection):
        self.f = open('pythonCoGroupOut.txt', 'a')
        self.__iter1 = Iterator(self.f, connection, -3)
        self.__iter2 = Iterator(self.f, connection, -4)
        self.__collector = Collector(self.f, connection)
    
    def log(self, s):
        self.f.write(str(s) + "\n")
        
    def coGroup(self, coGroupFunc):
        while(self.__iter1.hasMore() and self.__iter2.hasMore()):
            coGroupFunc(self.__iter1, self.__iter2, self.__collector)
            self.__collector.finishSingleCall()