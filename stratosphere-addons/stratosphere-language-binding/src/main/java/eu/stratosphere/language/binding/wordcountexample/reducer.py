#!/usr/bin/env pytho
 
import socket
import google.protobuf.internal.decoder as decoder
import google.protobuf.internal.encoder as encoder
import keyValue_pb2
 
HOST = "localhost"
INPORT = 8081
 
# TODO: Look if AF_INET could be AF_LOCAL and be quicker....
inSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
inSock.connect((HOST, INPORT))

READING_BYTES = 10
 
print "Python subprogram started"

while True:
    
    buf = inSock.recv(READING_BYTES)
    (size, position) = decoder._DecodeVarint(buf, 0)
    
    print "size: "+str(size)+" position: "+str(position)
    
    if size == 4294967295: # this is a hack my friend we probably need fixlength types for the length
		break
    
    # 1) Reading the keyValue pair from the socket
    toRead = size+position-READING_BYTES
    if toRead > 0:
        buf += inSock.recv(toRead) # this is probably inefficient because the buffer sizes changes all the time
    
    kvs = keyValue_pb2.KeyValueStream()
    kvs.ParseFromString(buf[position:position+size])
    
    sum = 0;
    lastRecord = None
    
    for kvp in kvs.record:
        print "Got kvp:  ("+ kvp.key + ":" + str(kvp.value) + ")"  
        sum += kvp.value
        lastRecord = kvp
    
    # Setup result
    result = keyValue_pb2.KeyValueStream()
    kvp = result.record.add()
    kvp.key = lastRecord.key
    kvp.value = sum
    
    print "Result kvp:  ("+ kvp.key + ":" + str(kvp.value) + ")"
    
    # 2) Reading the keyValue pair from the socket
    outBuf = result.SerializeToString()
    print "Sending back to java- outbuf-len: " + str(len(outBuf))
    buf = encoder._VarintBytes(len(outBuf))
    inSock.send(buf)
    inSock.send(outBuf)
    
print "Got -1, Finishing python process"