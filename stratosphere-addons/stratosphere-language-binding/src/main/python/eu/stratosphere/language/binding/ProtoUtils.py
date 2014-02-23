#from enum import Enum
import sys
import socket
import google.protobuf.internal.decoder as decoder
import google.protobuf.internal.encoder as encoder
import stratosphereRecord_pb2

# define our own enum type, because enums are only supported in python 3.4
def enum(**enums):
    return type('Enum', (), enums)        
ConnectionType = enum(STDPIPES=1, PIPES=2, SOCKETS=3)

SIGNAL_SINGLE_CALL_DONE = -1;
SIGNAL_ALL_CALLS_DONE = -2;

class Connection(object):
    def __init__(self, send_func, recv_func):
        self.__send_func = send_func
        self.__recv_func = recv_func
        
    def send(self, buffer):
        self.__send_func(buffer)
        
    def receive(self, size):
        return self.__recv_func(size)
    
    # Reads a size from the connection and returns it's value
    def readSize(self):
        size = stratosphereRecord_pb2.ProtoRecordSize()
        sizeBuf = self.receive(5)
        size.ParseFromString(sizeBuf)
        return size.value
    
    def sendSize(self, givenSize):
        size = stratosphereRecord_pb2.ProtoRecordSize()
        size.value = givenSize
        self.send(size.SerializeToString())
    
class SocketConnection(Connection):
    
    def __init__(self, host, port):
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.connect((host, port)) 
        super(SocketConnection, self).__init__(self.__sock.send, self.__sock.recv)       

class STDPipeConnection(Connection):
    
    def __init__(self): 
        super(STDPipeConnection, self).__init__(self.inner_send, sys.stdin.read) 
        
    def inner_send(self, buffer):
        sys.stdout.write(buffer)
        sys.stdout.flush()    
            
            
        
"""
    Class for receiving protobuf records and parsing them into python tuple.
    There are two ways to use it: 
    - call readSingleRecord() to only read one record - only used for mapper
    - iterate over the object. This is used for the reducer, that way
      the user get's the iterator and can that way receive one record after the other 
      inside the UDF.
"""
class Iterator(object):
        
    def __init__(self, f, connection, preIndex = None):
        self.finished = False
        self.f = f
        self.__connection = connection
        self.__preIndex = preIndex
        
    def log(self,s):
        self.f.write(str(s) + "\n")
        
    def hasMore(self):
        return not self.finished
    
    def __iter__(self): 
        while(True):
            self.log("calling iterator function")
            if(self.__preIndex != None):
                self.__connection.sendSize(self.__preIndex)
            self.log("sent " + str(self.__preIndex))
                
            size = self.__connection.readSize()
            self.log("gotSize " + str(size))
            # If we are done with this map/reduce/... record-collection we get a -1 from java
            if size == SIGNAL_SINGLE_CALL_DONE:
                break
            # If we get a -2 it means that the operator is done completely and we can close this process
            elif size == SIGNAL_ALL_CALLS_DONE:
                self.finished = True
                break
            # Otherwise we read the record, parse and yield it
            else:
                yield self.readRecord(size)
    
    # Function for reading a single record, should only be used by the Map-Operator
    def readSingleRecord(self):
        size = self.__connection.readSize()
        if size == SIGNAL_ALL_CALLS_DONE:
            self.finished = True
            return None
        else:
            return self.readRecord(size);
    
    # General help-function for reading and parsing records, without any signal handling 
    def readRecord(self, size):
        buf = self.__connection.receive(size)
        record = stratosphereRecord_pb2.ProtoStratosphereRecord()
        record.ParseFromString(buf)
        return self.getTuple(record)
    
    
    
    # Function which gets the ProtoStratosphereRecord (Record representation of the protocol buffer classes)
    # and returns the corresponding tuple
    def getTuple(self, record):
        tuple = ()
        for value in record.values:
            if(value.valueType == stratosphereRecord_pb2.ProtoStratosphereRecord.IntegerValue32):
                tuple += (value.int32Val,)
            elif(value.valueType == stratosphereRecord_pb2.ProtoStratosphereRecord.StringValue):
                tuple += (value.stringVal,)
            else:
                raise BaseException("A currently not implemented valueType")
        return tuple

"""
    Class for sending python tuples as protobuf records to the java process.
    A single tuple is sent via the collect() function. 
    When all records for a single map/reduce/... function are send the finish()
    function must be called to tell the java process that we're finished.
"""
class Collector(object):
        
    def __init__(self, f, connection):
        self.f = f
        self.__connection = connection

    def log(self,s):
        self.f.write(str(s) + "\n")
            
    def collect(self, tuple):
        
        result = self.getProtoRecord(tuple);
        recordBuf = result.SerializeToString()
        
        #encoder._EncodeVarint(sys.stdout.write, len(recordBuf))
        #sys.stdout.write(recordBuf)
        self.__connection.sendSize(len(recordBuf))
        self.__connection.send(recordBuf)

    def finishSingleCall(self):
        self.__connection.sendSize(SIGNAL_SINGLE_CALL_DONE)

    def finish(self):
        self.__connection.sendSize(SIGNAL_ALL_CALLS_DONE)

    # Function which gets the a python tuple and returns a 
    # ProtoStratosphereRecord (Record representation of the protocol buffer classes)
    def getProtoRecord(self, tuple):
        result = stratosphereRecord_pb2.ProtoStratosphereRecord()
        for value in tuple:
            protoVal = result.values.add()
            if isinstance(value, str) or isinstance(value, unicode):
                protoVal.valueType = stratosphereRecord_pb2.ProtoStratosphereRecord.StringValue
                protoVal.stringVal = value
            elif isinstance(value, int):
                protoVal.valueType = stratosphereRecord_pb2.ProtoStratosphereRecord.IntegerValue32
                protoVal.int32Val = value
            else:
                raise BaseException("A currently not implemented valueType")
        return result
    
"""class SocketConnection(object):
    def __init__(self, host, port):
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.connect((host, port))
    
    def send(self, buffer):
        self.__sock.send(buffer)
        
    def receive(self, size):
        return self.__sock.recv(size)
        
class STDPipeConnection(object):
    def __init__(self):
        pass
    
h
        
    def receive(self, size):
        return sys.stdin.read(size)"""