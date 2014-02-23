package eu.stratosphere.language.binding.java.Streaming;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.CoGroupFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.CoGroupOperator;
import eu.stratosphere.api.java.record.operators.CrossOperator;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.language.binding.java.operators.PyCoGroupFunction;
import eu.stratosphere.language.binding.java.operators.PyCrossFunction;
import eu.stratosphere.language.binding.java.operators.PyJoinFunction;
import eu.stratosphere.language.binding.java.operators.PyMapFunction;
import eu.stratosphere.language.binding.java.operators.PyReduceFunction;
import eu.stratosphere.language.binding.protos.StratospherePlan.ProtoPlan;
import eu.stratosphere.language.binding.protos.StratospherePlan.ProtoPlan.KeyValuePair;
import eu.stratosphere.language.binding.protos.StratospherePlan.ProtoPlan.ValueType;
import eu.stratosphere.language.binding.protos.StratospherePlan.ProtoPlan.Vertex;
import eu.stratosphere.language.binding.protos.StratospherePlan.ProtoPlan.VertexType;
import eu.stratosphere.language.binding.protos.StratosphereRecordProtoBuffers.ProtoRecordSize;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class PlanReceiver {

	private InputStream inStream;
	
	public PlanReceiver(InputStream inStream){
		this.inStream = inStream;
	}

	public Plan receivePlan(String scriptPath, ConnectionType connection) throws Exception{
		int size = getSize();
		byte[] buffer = new byte[size];
		inStream.read(buffer);
		ProtoPlan pp = ProtoPlan.parseFrom(buffer);
		System.out.println(pp);
		return getPlan(pp, scriptPath, connection);	
	}
	
	public int getSize() throws Exception{
		byte[] buf = new byte[5];
		inStream.read(buf);
		ProtoRecordSize size = ProtoRecordSize.parseFrom(buf);
		return size.getValue();
	}
	
	public List<Class<?extends Value>> getOutputClasses(Vertex vertex){
		if(vertex.getOutputTypesCount()== 0){
			return null;
		}
		
		List<Class<?extends Value>> result = new ArrayList<Class<? extends Value>>();
		for (int j = 0; j < vertex.getOutputTypesCount(); j++){
			ValueType type = vertex.getOutputTypes(j);
			switch (type) {
			case IntValue:
				result.add(IntValue.class);
				break;
			case StringValue:
				result.add(StringValue.class);
				break;
			default:
				break;
			}
		}
		
		return result;
	}
	
	private Plan getPlan(ProtoPlan protoPlan, String scriptPath, ConnectionType connection){
		Operator[] operators = new Operator[protoPlan.getVerticesCount()];
		@SuppressWarnings("unchecked")
		List<Class<?extends Value>> classes[] = new List[protoPlan.getVerticesCount()];
		
		for(int i = 0; i < protoPlan.getVerticesCount(); i++){
			Vertex vertex = protoPlan.getVertices(i);
			Map<String, String> params = new HashMap<String, String>();
			VertexType type = vertex.getType(); 
			List<Integer> in = vertex.getInputsList();
			classes[i] = getOutputClasses(vertex);
			
			for (int j = 0; j < vertex.getParamsCount(); j++){
				KeyValuePair param = vertex.getParams(j);
				params.put(param.getKey(), param.getValue());
			}
	
			System.out.println(type + "InputClasses: " + classes);
			switch(type){
				case TextInputFormat:
					operators[i] = new FileDataSource(new TextInputFormat(), 
						params.get("filePath"));
					break;
				case Map:
					PyMapFunction mapFunction = new PyMapFunction(scriptPath, connection, classes[in.get(0)], i);
					operators[i] = MapOperator.builder(mapFunction)
						.input(operators[in.get(0)])
						.build();
					break;
				case Reduce:
					PyReduceFunction reduceFunction = new PyReduceFunction(scriptPath, connection, classes[in.get(0)], i);
					operators[i] = ReduceOperator.builder(reduceFunction, StringValue.class, 0)
							.input(operators[in.get(0)])
							.build();
					break;
					
				case Join:
					PyJoinFunction joinFunction = new PyJoinFunction(scriptPath, connection, classes[in.get(0)], classes[in.get(1)], i);
					operators[i] = JoinOperator.builder(joinFunction, StringValue.class, 0, 0)
							.input1(operators[in.get(0)])
							.input2(operators[in.get(1)])
							.build();
					break;

				case Cross:
					PyCrossFunction crossFunction = new PyCrossFunction(scriptPath, connection, classes[in.get(0)], classes[in.get(1)], i);
					operators[i] = CrossOperator.builder(crossFunction)
							.input1(operators[in.get(0)])
							.input2(operators[in.get(1)])
							.build();
					break;
					
				case CoGroup:
					PyCoGroupFunction coGroup = new PyCoGroupFunction(scriptPath, connection, classes[in.get(0)], classes[in.get(1)], i);
					operators[i] = CoGroupOperator.builder(coGroup, StringValue.class, 0, 0)
							.input1(operators[in.get(0)])
							.input2(operators[in.get(1)])
							.build();
					break;
				case CsvOutputFormat:
					FileDataSink out = new FileDataSink(new CsvOutputFormat(), 
							params.get("filePath"), operators[vertex.getInputs(0)]);
					CsvOutputFormat.configureRecordFormat(out)
						.recordDelimiter('\n')
						.fieldDelimiter(params.get("fieldDelimiter").charAt(0))
						.field(StringValue.class, 0)
						.field(IntValue.class, 1);
					operators[i] = out;
					break;
				default:
					throw new RuntimeException("Not implemented Vertex/Operatortype");
			}
		}
		
		Plan result = new Plan((GenericDataSink)operators[protoPlan.getVerticesCount()-1], "Word Count Python Example");
		return result;
	}
}
