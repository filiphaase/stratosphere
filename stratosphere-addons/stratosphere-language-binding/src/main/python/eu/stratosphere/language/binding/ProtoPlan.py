from stratospherePlan_pb2 import ProtoPlan  
from ProtoUtils import STDPipeConnection
from ProtoMapper import Mapper
from ProtoReducer import Reducer
from ProtoJoinCross import JoinCross
from ProtoCoGroup import CoGroup
import sys

f = open("pythonPlanModuleOut.txt", "w")
def log(s):
    f.write(str(s) + "\n")
    
# define our own enum type, because enums are only supported in python 3.4
def enum(**enums):
    return type('Enum', (), enums)        
ValueType = enum(String=1, Int=2)

class Plan(object):
    
    def __init__(self):
        self.__protoPlan = None
        self.__vertices = []
        
    def __str__(self):
        return "Plan: " + str(self.__protoPlan)
    
    def appendVertex(self, vertex):
        self.__vertices.append(vertex)
        log("self.__vertices after appending: "  + str(self.__vertices) + "len: " + str(len(self.__vertices)))
        
    def getNewEmptyProtoVertex(self):
        return self.__protoPlan.vertices.add()
    
    def execute(self):
        """
            Receive an integer from connection and if it is -1 we have to send back
            the plan next...
            otherwise we have to execute the given vertex number
            The code is easier if we have the vertices always sorted here (according to the id)
        """
        log("in execute")
        self.__connection = STDPipeConnection()
        log("setup stdpipe connection")
        vertexInd = self.__connection.readSize()
        log("got size: "+ str(vertexInd))
        self.__vertices.sort()
        log("sorted")
        
        if vertexInd == -1:
            log("in sending plan back")
            #Setup protoplan
            self.__protoPlan = ProtoPlan()
            
            for vertex in self.__vertices:
                #Adding it to the plan, therefore we give a new vertex refernce to the function
                vertex.fillVertex(self.__protoPlan.vertices.add())
            log("really sending plan back")
            self.sendPlan(self.__connection)
            log("sent plan")
        else:
            vertex = self.__vertices[vertexInd]
            
            if vertex.vertexType == ProtoPlan.Map:
                Mapper(self.__connection).map(vertex.function)            
            elif vertex.vertexType == ProtoPlan.Reduce: 
                Reducer(self.__connection).reduce(vertex.function)
            elif vertex.vertexType == ProtoPlan.Join:
                JoinCross(self.__connection).join(vertex.function)
            elif vertex.vertexType == ProtoPlan.Cross:
                JoinCross(self.__connection).cross(vertex.function)
            elif vertex.vertexType == ProtoPlan.CoGroup:
                CoGroup(self.__connection).coGroup(vertex.function)
            
    def sendPlan(self, connection):
        log("### sending plan now")
        buf = self.__protoPlan.SerializeToString()
        log("### buf len" + str(len(buf)))
        connection.sendSize(len(buf))
        connection.send(buf)
        
    def mergePlan(self, otherPlan):
        log("Merging plans now: " + str(self) + "///and///" + str(otherPlan))
        self.__vertices += otherPlan.__vertices
        #TODO sort list because of IDs
        
class Vertex(object):
    
    curNumVertices = 0
    
    def __init__(self, vertexType, parent1 = None, parent2 = None, params = {}, function = None, types = []):
        self.CONST_PARAM_FILE_PATH = "filePath"
        self.CONST_PARAM_FIELD_DELIMITER = "fieldDelimiter"
        self.CONST_PARAM_RECORD_DELIMITER = "recordDelimiter"
        self.CONST_PARAM_KEY_INDEX_1 = "keyIndex1"
        self.CONST_PARAM_KEY_INDEX_2 = "keyIndex2"
        self.CONST_PARAM_INDEX_LIST = "indexList"
        
        self.ind = Vertex.curNumVertices
        Vertex.curNumVertices += 1
        
        self.vertexType = vertexType
        if parent1 == None:
            self.plan = Plan()
        else:
            self.plan = parent1.plan
        
        log("initVertex-parent1: " + str(parent1))
        log("initVertex-parent2: " + str(parent2))
        if parent2 != None:
            self.plan.mergePlan(parent2.plan)
            
        self.__outputTypes = types
        self.__params = params
        self.function = function
        self.inputs = []
        if parent1 != None:
           self.inputs.append(parent1.ind)
        if parent2 != None:
           self.inputs.append(parent2.ind)
           
        self.plan.appendVertex(self)
        #self.addVertexToPlan()
        
    def __cmp__(self, other):
        if other == None:
            return -1
  
        return self.ind - other.ind
    
    def map(self, f, valueTypes):
        mapVertex = Vertex(ProtoPlan.Map, parent1=self, function = f, types = valueTypes)
        return mapVertex
        
    def reduce(self, f, valueTypes, keyInd = 0):
        reduceVertex = Vertex(ProtoPlan.Reduce, parent1=self,  function = f, types = valueTypes)
        return reduceVertex.key(keyInd)
    
    def join(self, otherParent, f, valueTypes, keyInd1 = 0, keyInd2 = 1):
        joinVertex = Vertex(ProtoPlan.Join, parent1=self, parent2=otherParent, function = f, types = valueTypes )
        return joinVertex.keys(keyInd1, keyInd2)
    
    def cross(self, otherParent, f, valueTypes):
        crossVertex = Vertex(ProtoPlan.Cross, parent1=self, parent2=otherParent, function = f, types = valueTypes )
        return crossVertex

    def coGroup(self, otherParent, f, valueTypes, keyInd1 = 0, keyInd2 = 1):
        coGroupVertex = Vertex(ProtoPlan.CoGroup, parent1=self, parent2=otherParent, function = f, types = valueTypes )
        return coGroupVertex.keys(keyInd1, keyInd2)
    
    def outputCSV(self, filePath, indices, recordDelimiter = '\n', fieldDelimiter = " "):
        outputVertex = Vertex(ProtoPlan.CsvOutputFormat, parent1=self)
        return outputVertex.file(filePath).indices(indices).fieldDelimiter(fieldDelimiter).recordDelimiter(recordDelimiter)
    
    def fillVertex(self, newVertex):
         newVertex.type = self.vertexType
         newVertex.inputs.extend(self.inputs)
        
         log("Inputs: " + str(self.inputs))
         log("VertexSelf: " + str(self)) 
         for key,value in self.__params.iteritems():
             kvp = newVertex.params.add()
             kvp.key = key
             kvp.value = value
         
         protoTypes = []
         for type in self.__outputTypes:
             if type == ValueType.String:
                 protoTypes.append(ProtoPlan.StringValue)
             elif type == ValueType.Int:
                 protoTypes.append(ProtoPlan.IntValue)
         newVertex.outputTypes.extend(protoTypes)
         
         log("VertexSelfProtobuf: " + str(newVertex))
    
    def planString(self):
        return str(self.plan)

    def __str__(self):
        return str(self.ind) + " "+ str(self.vertexType) + ((" Params: " + str(self.__params)) if self.__params != None else "")

    def execute(self):
        log("executing now")
        self.plan.execute()
        
    def key(self, ind):
        self.__params[self.CONST_PARAM_KEY_INDEX_1] = str(ind)
        return self

    def keys(self, ind1, ind2):
        self.__params[self.CONST_PARAM_KEY_INDEX_1] = str(ind1)
        self.__params[self.CONST_PARAM_KEY_INDEX_2] = str(ind2)
        return self
    
    def file(self, path):
        self.__params[self.CONST_PARAM_FILE_PATH] = path
        return self
    
    def fieldDelimiter(self, delimiter):
        self.__params[self.CONST_PARAM_FIELD_DELIMITER] = delimiter
        return self
        
    def recordDelimiter(self, delimiter):
        self.__params[self.CONST_PARAM_RECORD_DELIMITER] = delimiter
        return self
    
    def indices(self, indices):
        self.__params[self.CONST_PARAM_INDEX_LIST] = ','.join(str(i) for i in indices)
        return self
            
class TextInputFormat(Vertex):    
    def __init__(self, inputPath):
        super(TextInputFormat, self).__init__(ProtoPlan.TextInputFormat, types = [ValueType.String], params = {"filePath": inputPath})
