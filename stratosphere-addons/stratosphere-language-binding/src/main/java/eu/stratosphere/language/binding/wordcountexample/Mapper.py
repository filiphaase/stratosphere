# !/usr/bin/env pytho
 
import socket
import google.protobuf.internal.decoder as decoder
import google.protobuf.internal.encoder as encoder
import keyValue_pb2
import re
import sys
 
HOST = "localhost"
INPORT = int(sys.argv[1])
 
# TODO: Look if AF_INET could be AF_LOCAL and be quicker....
inSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
inSock.connect((HOST, INPORT))
f = open("workfile", "w")
READING_BYTES = 10
 
print "Python subprogram started"
while True:
    buf = inSock.recv(READING_BYTES)
    (size, position) = decoder._DecodeVarint(buf, 0)

    f.write("received something\n")        
    if size == 4294967295: # this is a hack my friend we probably need fixlength types for the length
        break
        # 1) Reading the keyValue pair from the socket
    toRead = size + position - READING_BYTES
    if toRead > 0:
        buf += inSock.recv(toRead)  # this is probably inefficient because the buffer sizes changes all the time
        
    kv = keyValue_pb2.KeyValuePair()
    kv.ParseFromString(buf[position:position + size])
    line = kv.key
    line = line.lower()
    line = re.sub(r"\W+", " ", line)

    kvs = keyValue_pb2.KeyValueStream()
    for text in line.split():
        kvp = kvs.record.add()
        kvp.key = text
        kvp.value = 1
        
    #if sendMinOne:
    
    # 2) Reading the keyValue pair from the socket
    outBuf = kvs.SerializeToString()
    buf = encoder._VarintBytes(len(outBuf))
    inSock.send(buf)
    inSock.send(outBuf)
        
    """else:
        
        print "Need to send something for null"
        buf = encoder._VarintBytes(4294967295)
        print buf
        inSock.send(buf)"""
        