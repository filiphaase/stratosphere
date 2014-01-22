#!/usr/bin/env pytho
 
import socket
import google.protobuf.internal.decoder as decoder
import google.protobuf.internal.encoder as encoder
import keyValue_pb2
import sys
import time
 
sys.stderr = open('pythonReducerError.txt', 'w')
#sys.stdout = open('pythonReducerOut.txt', 'w')

HOST = "localhost"
INPORT = 8081
useSockets = False

if useSockets:
    # TODO: Look if AF_INET could be AF_LOCAL and be quicker....
    inSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    inSock.connect((HOST, INPORT))
    READING_BYTES = 10
else:
    #TODO Why the hell? :D
    READING_BYTES = 5
    
sum = 0
kvp = None
buf = b''

while True:

    # if there is still something in the buffer, just read so much that we have exactly READING_BYTES
    # len(buf) can maximal be READING_BYTES so this is never negative
    if not useSockets:
        buf += sys.stdin.read(READING_BYTES-len(buf))
    else:
        buf += inSock.recv(READING_BYTES-len(buf))
    
    (size, position) = decoder._DecodeVarint(buf, 0)
    
    # If we are done with this keyValue pair we get a -1
    # this is a hack my friend we probably need fixlength types for the length
    if size == 4294967295:
        result = keyValue_pb2.KeyValuePair()
        result.key = kvp.key
        result.value = sum
        
        # 2) Reading the keyValue pair from the socket
        outBuf = result.SerializeToString()
        buf = encoder._VarintBytes(len(outBuf))
        if not useSockets:
            sys.stdout.write(buf)
            sys.stdout.write(outBuf)
            sys.stdout.flush()
        else:
            inSock.send(buf)
            inSock.send(outBuf)
    
        sum = 0
        buf= b''
        
        continue
    
    # For printing reasons, end the process by sent size -2 
    #if size == 4294967294:
    #    break
    
    # 1) Reading the keyValue pair from the socket
    toRead = size+position-len(buf)
    # if the message exceeds what we already read, read the rest
    if toRead > 0:
        if not useSockets:
            buf += sys.stdin.read(toRead)
        else:
            buf += inSock.recv(toRead) # this is probably inefficient because the buffer sizes changes all the time
    
    kvp = keyValue_pb2.KeyValuePair()
    kvp.ParseFromString(buf[position:position+size])
    sum += kvp.value;
    
    # throw away the part of the buffer which we already processed
    if toRead < 0:
        buf = buf[(position+size):len(buf)]
    else:
        buf = b''