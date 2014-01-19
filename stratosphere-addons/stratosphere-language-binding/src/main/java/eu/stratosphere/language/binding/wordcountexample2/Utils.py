#from enum import Enum
import sys
import google.protobuf.internal.decoder as decoder
import google.protobuf.internal.encoder as encoder
import stratosphereRecord_pb2

#Something for output, we need it :(
#sys.stderr = open('pythonReducerError.txt', 'w')
    
def getTuple(record):
    t = ()
    for value in record.values:
        if(value.valueType == stratosphereRecord_pb2.ProtoStratosphereRecord.IntegerValue32):
            t += (value.int32Val,)
        elif(value.valueType == stratosphereRecord_pb2.ProtoStratosphereRecord.StringValue):
            t += (value.stringVal,)
        else:
            raise BasisException("A currently not implemented valueType")
    return t
        

#class ConnectionType(enum):
#    SOCKET = 1
#    STDPIPE = 2

class Iterator(object):
        
    def __init__(self, f):#, conn):
        f.write("Initializing Iterator")
        self.f = f
        self.log("Successfully initialized Iterator")
        #self.__connectionType = conn
        #if conn != ConnectionType.STDPIPE:
            #raise BaseException("Not implemented so far")
        
    def log(self,s):
        self.f.write(str(s) + "\n")
        
    def __iter__(self): 
        while(True):
            size = stratosphereRecord_pb2.ProtoRecordSize()
            sizeBuf = sys.stdin.read(5)
            size.ParseFromString(sizeBuf)
    
            # If we are done with this keyValue pair we get a -1
            if size.value == 4294967295:
                break
            else:            
                buf = sys.stdin.read(size.value)
    
                record = stratosphereRecord_pb2.ProtoStratosphereRecord()
                record.ParseFromString(buf)
                yield getTuple(record)
    
class Collector(object):
        
    #def __init__(self, conn):
    #    self.__connectionType = conn
    #    
    #    if conn != ConnectionType.STDPIPE:
    #        raise BaseException("Not implemented so far")
    
    def __init__(self, f):
        self.f = f

    def log(self,s):
        self.f.write(str(s) + "\n")
            
    def collect(self, tuple):
        
        result = stratosphereRecord_pb2.ProtoStratosphereRecord()
        value = result.values.add()
        value.valueType = stratosphereRecord_pb2.ProtoStratosphereRecord.StringValue
        value.stringVal = tuple[0]
        
        value = result.values.add()
        value.valueType = stratosphereRecord_pb2.ProtoStratosphereRecord.IntegerValue32
        value.int32Val = tuple[1]
        
        recordBuf = result.SerializeToString()
        
        sizeBuf = encoder._VarintBytes(len(recordBuf))
        sys.stdout.write(sizeBuf)
        sys.stdout.write(recordBuf)
        sys.stdout.flush()            
