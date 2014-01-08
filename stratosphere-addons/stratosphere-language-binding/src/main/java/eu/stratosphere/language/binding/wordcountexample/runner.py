#!/usr/bin/env pytho
 
import socket
import google.protobuf.internal.decoder as decoder
import google.protobuf.internal.encoder as encoder
import keyValue_pb2
 
HOST = "localhost"
INPORT = 8080
 
# TODO: Look if AF_INET could be AF_LOCAL and be quicker....
inSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
inSock.connect((HOST, INPORT))

READING_BYTES = 10
 
print "Python subprogram started"
while True:
    
    buf = inSock.recv(READING_BYTES)
    (size, position) = decoder._DecodeVarint(buf, 0)
    
    print '\n' + "size: "+str(size)+" position: "+str(position) + '\n'
    
    if size == 4294967295: # this is a hack my friend we probably need fixlength types for the length
		break
    
    # 1) Reading the keyValue pair from the socket
    toRead = size+position-READING_BYTES
    buf += inSock.recv(toRead) # this is probably inefficient because the buffer sizes changes all the time
    print("bufSize "+str(len(buf)))
    kv = keyValue_pb2.KeyValuePair()
    kv.ParseFromString(buf[position:position+size])
    print("key "+kv.key)
    print("value "+kv.value)
    
    kvs = keyValue_pb2.KeyValueStream()
    kvp = kvs.record.add()
    kvp.key = "key1"
    kvp.value = "value1"
    
    kvp = kvs.record.add()
    kvp.key = "key2"
    kvp.value = "value2"
    
    # 2) Reading the keyValue pair from the socket
    outBuf = kvs.SerializeToString()
    print "Sending back to java- outbuf-len: " + str(len(outBuf))
    buf = encoder._VarintBytes(len(outBuf))
    inSock.send(buf)
    inSock.send(outBuf)
    
print "Got -1, Finishing python process"