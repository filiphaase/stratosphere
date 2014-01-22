#from enum import Enum
import sys
import google.protobuf.internal.decoder as decoder
import google.protobuf.internal.encoder as encoder
import stratosphereRecord_pb2

def err_log(s):
    sys.stderr.write(s + "\n")

# define our own enum type, because enums are only supported in python 3.4
def enum(**enums):
    return type('Enum', (), enums)        
ConnectionType = enum(STDPIPES=1, PIPES=2, SOCKETS=3)
    
# Function which gets the ProtoStratosphereRecord (Record representation of the protocol buffer classes)
# and returns the corresponding tuple
def getTuple(record):
    tuple = ()
    for value in record.values:
        if(value.valueType == stratosphereRecord_pb2.ProtoStratosphereRecord.IntegerValue32):
            tuple += (value.int32Val,)
        elif(value.valueType == stratosphereRecord_pb2.ProtoStratosphereRecord.StringValue):
            tuple += (value.stringVal,)
        else:
            raise BaseException("A currently not implemented valueType")
    return tuple

# Function which gets the a python tuple and returns a 
# ProtoStratosphereRecord (Record representation of the protocol buffer classes)
def getProtoRecord(tuple):
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

"""
This would be a way to read a var int without reading more bytes then needed 
def readVarInt(iter):
    buf = []
    
    for i in xrange(0,9):
        buf += sys.stdin.read(1)
        iter.log(bin(ord(buf[i])))
        if (ord(buf[i]) & (0x01 << 7)) == 0:
            return buf[0:(i+1)]
        
    # Should never happen
    iter.log('return')
    return buf"""

class Iterator(object):
        
    def __init__(self, f, conn):
        self.finished = False
        self.f = f
        self.__connectionType = conn
        if conn != ConnectionType.STDPIPES:
            raise BaseException("Not implemented so far")
        
    def log(self,s):
        self.f.write(str(s) + "\n")
        
    def hasMore(self):
        return not self.finished
    
    def __iter__(self): 
        while(True):
            size = self.readSize()
            # If we are done with this map/reduce/... record-collection we get a -1
            if size == 4294967295:
                break
            # If we get a -2 it means that the operator is done completely and we can close this process
            elif size == 4294967294:
                self.finished = True
                break
            # Otherwise we read the record, parse and return it
            else:
                yield self.readRecord(size)
    
    # Function for reading a single record, should only be used by the Map-Operator
    def readSingleRecord(self):
        size = self.readSize()
        if size == 4294967294:
            self.finished = True
            return None
        else:
            return self.readRecord(size)
        
    def readSize(self):
        size = stratosphereRecord_pb2.ProtoRecordSize()
        sizeBuf = sys.stdin.read(5)
        size.ParseFromString(sizeBuf)
        return size.value
        #sizeBuf = readVarInt(self)
        #(size, position) = decoder._DecodeVarint(sizeBuf, 0)
        
    def readRecord(self, size):
        buf = sys.stdin.read(size)
        record = stratosphereRecord_pb2.ProtoStratosphereRecord()
        record.ParseFromString(buf)
        return getTuple(record)
        
class Collector(object):
        
    def __init__(self, f, conn):
        self.f = f
        self.__connectionType = conn
        
        if conn != ConnectionType.STDPIPES:
            raise BaseException("Not implemented so far")

    def log(self,s):
        self.f.write(str(s) + "\n")
            
    def collect(self, tuple):
        
        result = getProtoRecord(tuple);
        recordBuf = result.SerializeToString()
        
        #encoder._EncodeVarint(sys.stdout.write, len(recordBuf))
        #sys.stdout.write(recordBuf)
        
        size = stratosphereRecord_pb2.ProtoRecordSize()
        size.value = len(recordBuf)
        sys.stdout.write(size.SerializeToString() + recordBuf)
        sys.stdout.flush()        

    def finish(self):
        size = stratosphereRecord_pb2.ProtoRecordSize()
        size.value = 4294967295
        sys.stdout.write(size.SerializeToString())
        sys.stdout.flush()