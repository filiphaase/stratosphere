from stratospherePlan_pb2 import ProtoPlan  
from ProtoUtils import STDPipeConnection
from ProtoMapper import Mapper
from ProtoReducer import Reducer
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
        self.__protoPlan = ProtoPlan()
        self.__vertices = []
        
    def __str__(self):
        return "Plan: " + str(self.__protoPlan)
    
    def getNewEmptyProtoVertexAndAddVertex(self, vertex):
        self.__vertices.append(vertex)
        return self.__protoPlan.vertices.add()
    
    def execute(self):
        """
            Receive a int from connection and if it is -1 we have to send back
            the plan next...
            otherwise we have to execute the given vertex number
        """
        self.__connection = STDPipeConnection()
        vertexInd = self.__connection.readSize()
        if vertexInd == 4294967295:
            self.sendPlan(self.__connection)
        else:
            vertex = self.__vertices[vertexInd]
            
            if vertex.vertexType == ProtoPlan.Map:
                Mapper(self.__connection).map(vertex.function)            
            elif vertex.vertexType == ProtoPlan.Reduce: 
                Reducer(self.__connection).reduce(vertex.function)

            
    def sendPlan(self, connection):
        buf = self.__protoPlan.SerializeToString()
        connection.sendSize(len(buf))
        connection.send(buf)
        
class Vertex(object):
    
    curNumVertices = 0
    
    def __init__(self, vertexType, parent1 = None, parent2 = None, params = {}, function = None, types = []):
        self.ind = Vertex.curNumVertices
        Vertex.curNumVertices += 1
        
        self.vertexType = vertexType
        if parent1 == None:
            self.plan = Plan()
        else:
            self.plan = parent1.plan
        
        self.__inputTypes = types
        self.__params = params
        self.function = function
        self.inputs = []
        if parent1 != None:
           self.inputs.append(parent1.ind)
        if parent2 != None:
           self.inputs.append(parent2.ind)
           
        self.addVertexToPlan()
        
    def map(self, f, valueTypes):
        mapVertex = Vertex(ProtoPlan.Map, parent1=self, function = f, types = valueTypes)
        return mapVertex
        
    def reduce(self, f, valueTypes):
        reduceVertex = Vertex(ProtoPlan.Reduce, parent1=self,  function = f, types = valueTypes)
        return reduceVertex
    
    def outputCSV(self, filePath, valueTypes, delimiter = "/n", fieldDelimiter = " "):
        outputVertex = Vertex(ProtoPlan.CsvOutputFormat, parent1=self, types = valueTypes,  params = {"filePath": filePath, "delimiter" : delimiter, "fieldDelimiter" : fieldDelimiter})
        return outputVertex
    
    def addVertexToPlan(self):
         newVertex = self.plan.getNewEmptyProtoVertexAndAddVertex(self)
         newVertex.type = self.vertexType
         newVertex.inputs.extend(self.inputs)
         
         for key,value in self.__params.iteritems():
             kvp = newVertex.params.add()
             kvp.key = key
             kvp.value = value
         
         protoTypes = []
         for type in self.__inputTypes:
             if type == ValueType.String:
                 protoTypes.append(ProtoPlan.StringValue)
             elif type == ValueType.Int:
                 protoTypes.append(ProtoPlan.IntValue)
         newVertex.outputTypes.extend(protoTypes)
    
    def planString(self):
        return str(self.plan)

    def __str__(self):
        return str(self.ind) + " "+ str(self.vertexType) + ((" Params: " + str(self.__params)) if self.__params != None else "")

    def execute(self):
        self.plan.execute()
             
class TextInputFormat(Vertex):    
    def __init__(self, inputPath):
        super(TextInputFormat, self).__init__(ProtoPlan.TextInputFormat, params = {"filePath": inputPath})