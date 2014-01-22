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

READING_BYTES = 5
sum = 0
kvp = None
buf = b''

timeReceive1 = 0
timeReceive2 = 0
timeSent = 0

ts1 = time.time()

while True:

    # if there is still something in the buffer, just read so much that we have exactly READING_BYTES
    # len(buf) can maximal be READING_BYTES so this is never negative
    tmp = time.time()
   # sys.stderr.write("len of buf 1: " + str(len(buf))+ "\n")
    if not useSockets:
        sys.stderr.write("want to read: " + str(READING_BYTES-len(buf)) + "\n" )
        buf += sys.stdin.read(READING_BYTES-len(buf))
        sys.stderr.write("got something through stdin, len: " + str(len(buf)) + "\n")
    else:
        buf += inSock.recv(READING_BYTES-len(buf))
    timeReceive1 += (time.time()-tmp)
    
   # sys.stderr.write("len of buf 2: " + str(len(buf)) + "\n")
    if len(buf) == 0:
        size = 4294967295
    else:
        tmp = time.time() 
        (size, position) = decoder._DecodeVarint32(buf, 0)
        timeReceive2 += (time.time()-tmp)
    
    # If we are done with this keyValue pair we get a -1
    # this is a hack my friend we probably need fixlength types for the length
    if size == 4294967295:
        sys.stderr.write("got -1")
        result = keyValue_pb2.KeyValuePair()
        result.key = kvp.key
        result.value = sum
        
        # 2) Reading the keyValue pair from the socket
        tmp = time.time()
        outBuf = result.SerializeToString()
        buf = encoder._VarintBytes(len(outBuf))
        if not useSockets:
            sys.stderr.write("want to sent something through stdout")
            sys.stdout.write(buf)
            sys.stdout.write(outBuf)
            sys.stderr.write("want to sent something through stdout")
        else:
            inSock.send(buf)
            inSock.send(outBuf)
        timeSent += (time.time()-tmp)
    
        sum = 0
        buf= b''
        
        sys.stderr.write("TimeReceived so far: " + str(timeReceive1))
        sys.stderr.write("TimeDecoded so far: " + str(timeReceive2))
        sys.stderr.write("TimeSent so far: " + str(timeSent))
        sys.stderr.write("TimeTotal so far: " + str(time.time() - ts1))
        sys.stderr.write("")
        continue
    
    # For printing reasons, end the process by sent size -2 
    #if size == 4294967294:
    #    break
    
    # 1) Reading the keyValue pair from the socket
    toRead = size+position-READING_BYTES
    sys.stderr.write("still need to read: " + str(toRead) + "\n")
    # if the message exceeds what we already read, read the rest
    if toRead > 0:
        if not useSockets:
            buf += sys.stdin.read(toRead)
        else:
            buf += inSock.recv(toRead) # this is probably inefficient because the buffer sizes changes all the time
    
    kvp = keyValue_pb2.KeyValuePair()
    sys.stderr.write(str(kvp) + "\n");
    kvp.ParseFromString(buf[position:position+size])
    sum += kvp.value;
    
    # throw away the part of the buffer which we already processed
    if toRead < 0:
        buf = buf[(position+size):len(buf)]
    else:
        buf = b''