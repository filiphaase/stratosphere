# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: stratospherePlan.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)




DESCRIPTOR = _descriptor.FileDescriptor(
  name='stratospherePlan.proto',
  package='eu.stratosphere.language.binding.protos',
  serialized_pb='\n\x16stratospherePlan.proto\x12\'eu.stratosphere.language.binding.protos\"\xae\x06\n\tProtoPlan\x12K\n\x08vertices\x18\x01 \x03(\x0b\x32\x39.eu.stratosphere.language.binding.protos.ProtoPlan.Vertex\x12\x13\n\x0b\x44\x61taSinkIDs\x18\x02 \x03(\x05\x1a*\n\x0cKeyValuePair\x12\x0b\n\x03key\x18\x01 \x02(\t\x12\r\n\x05value\x18\x02 \x02(\t\x1a\x99\x02\n\x06Vertex\x12K\n\x04type\x18\x01 \x02(\x0e\x32=.eu.stratosphere.language.binding.protos.ProtoPlan.VertexType\x12\x12\n\x06inputs\x18\x02 \x03(\x05\x42\x02\x10\x01\x12Q\n\x0boutputTypes\x18\x03 \x03(\x0e\x32<.eu.stratosphere.language.binding.protos.ProtoPlan.ValueType\x12O\n\x06params\x18\x04 \x03(\x0b\x32?.eu.stratosphere.language.binding.protos.ProtoPlan.KeyValuePair\x12\n\n\x02ID\x18\x05 \x02(\x05\"L\n\tValueType\x12\x0f\n\x0bStringValue\x10\x00\x12\x0c\n\x08IntValue\x10\x01\x12\x10\n\x0c\x42ooleanValue\x10\x02\x12\x0e\n\nFloatValue\x10\x03\"\xa8\x02\n\nVertexType\x12\x07\n\x03Map\x10\x00\x12\n\n\x06Reduce\x10\x01\x12\x08\n\x04Join\x10\x02\x12\t\n\x05\x43ross\x10\x03\x12\x0b\n\x07\x43oGroup\x10\x04\x12\t\n\x05Union\x10\x05\x12\x13\n\x0fTextInputFormat\x10\x06\x12\x12\n\x0e\x43svInputFormat\x10\x07\x12\x18\n\x14\x44\x65limitedInputFormat\x10\x08\x12\x13\n\x0f\x46ileInputFormat\x10\t\x12\x13\n\x0fJDBCInputFormat\x10\n\x12\x13\n\x0f\x43svOutputFormat\x10\x0b\x12\x19\n\x15\x44\x65limitedOutputFormat\x10\x0c\x12\x14\n\x10\x46ileOutputFormat\x10\r\x12\x11\n\rBulkIteration\x10\x0e\x12\x12\n\x0e\x44\x65ltaIteration\x10\x0f\x42\x12\x42\x10StratospherePlan')



_PROTOPLAN_VALUETYPE = _descriptor.EnumDescriptor(
  name='ValueType',
  full_name='eu.stratosphere.language.binding.protos.ProtoPlan.ValueType',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='StringValue', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='IntValue', index=1, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='BooleanValue', index=2, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='FloatValue', index=3, number=3,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=507,
  serialized_end=583,
)

_PROTOPLAN_VERTEXTYPE = _descriptor.EnumDescriptor(
  name='VertexType',
  full_name='eu.stratosphere.language.binding.protos.ProtoPlan.VertexType',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='Map', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Reduce', index=1, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Join', index=2, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Cross', index=3, number=3,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='CoGroup', index=4, number=4,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='Union', index=5, number=5,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='TextInputFormat', index=6, number=6,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='CsvInputFormat', index=7, number=7,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='DelimitedInputFormat', index=8, number=8,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='FileInputFormat', index=9, number=9,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='JDBCInputFormat', index=10, number=10,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='CsvOutputFormat', index=11, number=11,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='DelimitedOutputFormat', index=12, number=12,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='FileOutputFormat', index=13, number=13,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='BulkIteration', index=14, number=14,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='DeltaIteration', index=15, number=15,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=586,
  serialized_end=882,
)


_PROTOPLAN_KEYVALUEPAIR = _descriptor.Descriptor(
  name='KeyValuePair',
  full_name='eu.stratosphere.language.binding.protos.ProtoPlan.KeyValuePair',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='eu.stratosphere.language.binding.protos.ProtoPlan.KeyValuePair.key', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='value', full_name='eu.stratosphere.language.binding.protos.ProtoPlan.KeyValuePair.value', index=1,
      number=2, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=179,
  serialized_end=221,
)

_PROTOPLAN_VERTEX = _descriptor.Descriptor(
  name='Vertex',
  full_name='eu.stratosphere.language.binding.protos.ProtoPlan.Vertex',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='type', full_name='eu.stratosphere.language.binding.protos.ProtoPlan.Vertex.type', index=0,
      number=1, type=14, cpp_type=8, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='inputs', full_name='eu.stratosphere.language.binding.protos.ProtoPlan.Vertex.inputs', index=1,
      number=2, type=5, cpp_type=1, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=_descriptor._ParseOptions(descriptor_pb2.FieldOptions(), '\020\001')),
    _descriptor.FieldDescriptor(
      name='outputTypes', full_name='eu.stratosphere.language.binding.protos.ProtoPlan.Vertex.outputTypes', index=2,
      number=3, type=14, cpp_type=8, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='params', full_name='eu.stratosphere.language.binding.protos.ProtoPlan.Vertex.params', index=3,
      number=4, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='ID', full_name='eu.stratosphere.language.binding.protos.ProtoPlan.Vertex.ID', index=4,
      number=5, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=224,
  serialized_end=505,
)

_PROTOPLAN = _descriptor.Descriptor(
  name='ProtoPlan',
  full_name='eu.stratosphere.language.binding.protos.ProtoPlan',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='vertices', full_name='eu.stratosphere.language.binding.protos.ProtoPlan.vertices', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='DataSinkIDs', full_name='eu.stratosphere.language.binding.protos.ProtoPlan.DataSinkIDs', index=1,
      number=2, type=5, cpp_type=1, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_PROTOPLAN_KEYVALUEPAIR, _PROTOPLAN_VERTEX, ],
  enum_types=[
    _PROTOPLAN_VALUETYPE,
    _PROTOPLAN_VERTEXTYPE,
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=68,
  serialized_end=882,
)

_PROTOPLAN_KEYVALUEPAIR.containing_type = _PROTOPLAN;
_PROTOPLAN_VERTEX.fields_by_name['type'].enum_type = _PROTOPLAN_VERTEXTYPE
_PROTOPLAN_VERTEX.fields_by_name['outputTypes'].enum_type = _PROTOPLAN_VALUETYPE
_PROTOPLAN_VERTEX.fields_by_name['params'].message_type = _PROTOPLAN_KEYVALUEPAIR
_PROTOPLAN_VERTEX.containing_type = _PROTOPLAN;
_PROTOPLAN.fields_by_name['vertices'].message_type = _PROTOPLAN_VERTEX
_PROTOPLAN_VALUETYPE.containing_type = _PROTOPLAN;
_PROTOPLAN_VERTEXTYPE.containing_type = _PROTOPLAN;
DESCRIPTOR.message_types_by_name['ProtoPlan'] = _PROTOPLAN

class ProtoPlan(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType

  class KeyValuePair(_message.Message):
    __metaclass__ = _reflection.GeneratedProtocolMessageType
    DESCRIPTOR = _PROTOPLAN_KEYVALUEPAIR

    # @@protoc_insertion_point(class_scope:eu.stratosphere.language.binding.protos.ProtoPlan.KeyValuePair)

  class Vertex(_message.Message):
    __metaclass__ = _reflection.GeneratedProtocolMessageType
    DESCRIPTOR = _PROTOPLAN_VERTEX

    # @@protoc_insertion_point(class_scope:eu.stratosphere.language.binding.protos.ProtoPlan.Vertex)
  DESCRIPTOR = _PROTOPLAN

  # @@protoc_insertion_point(class_scope:eu.stratosphere.language.binding.protos.ProtoPlan)


DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), 'B\020StratospherePlan')
_PROTOPLAN_VERTEX.fields_by_name['inputs'].has_options = True
_PROTOPLAN_VERTEX.fields_by_name['inputs']._options = _descriptor._ParseOptions(descriptor_pb2.FieldOptions(), '\020\001')
# @@protoc_insertion_point(module_scope)
