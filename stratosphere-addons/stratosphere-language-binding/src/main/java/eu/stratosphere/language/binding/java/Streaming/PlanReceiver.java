package eu.stratosphere.language.binding.java.Streaming;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.io.Files;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.Operator;
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
import eu.stratosphere.types.BooleanValue;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class PlanReceiver {

	private final static String PARAM_FILE_PATH = "filePath";
	private final static String PARAM_FIELD_DELIMITER = "fieldDelimiter";
	private final static String PARAM_RECORD_DELIMITER = "recordDelimiter";
	private final static String PARAM_KEY_INDEX_1 = "keyIndex1";
	private final static String PARAM_KEY_INDEX_2 = "keyIndex2";
	private final static String PARAM_INDEX_LIST = "indexList";
	private InputStream inStream;
	
	public PlanReceiver(InputStream inStream){
		this.inStream = inStream;
	}

	/**
	 * Function get's receives and parses a plan from the subprocess
	 * 
	 * @param scriptPath
	 * @param connection
	 * @param pythonCode
	 * @param frameworkPath
	 * @return
	 * @throws Exception
	 */
	public Plan receivePlan(String scriptPath, ConnectionType connection, String pythonCode, String frameworkPath) throws Exception{
		int size = getSize();
		byte[] buffer = new byte[size];
		inStream.read(buffer);
		ProtoPlan pp = ProtoPlan.parseFrom(buffer);
		return getPlan(pp, scriptPath, connection, pythonCode, frameworkPath);	
	}
	
	public int getSize() throws Exception{
		byte[] buf = new byte[5];
		inStream.read(buf);
		ProtoRecordSize size = ProtoRecordSize.parseFrom(buf);
		return size.getValue();
	}
	
	private Class<?extends Value>[] getOutputClasses(Vertex vertex){
		if(vertex.getOutputTypesCount()== 0){
			return null;
		}
		@SuppressWarnings("unchecked")
		Class<?extends Value>[] result = new Class[vertex.getOutputTypesCount()];
		for (int j = 0; j < vertex.getOutputTypesCount(); j++){
			result[j] = getClass(vertex.getOutputTypes(j));
		}	
		return result;
	}
	
	private Class<?extends Value> getClass(ValueType type){
		switch (type) {
			case IntValue:
				return IntValue.class;
			case StringValue:
				return StringValue.class;
			case BooleanValue:
				return BooleanValue.class;
			case FloatValue:
				return FloatValue.class;
			default:
				throw new RuntimeException("Unimplemented Type in python-language-binding");
		}
	}
	
	/**
	 * This function really constructs the stratosphere Plan from the data received from the subprocess
	 * 
	 * Therefore a 
	 */
	@SuppressWarnings("unchecked")
	private Plan getPlan(ProtoPlan protoPlan, String scriptPath, ConnectionType connection, String pythonCode, String frameworkPath) throws IOException{
		// Maps used to save the operators and their output classes according to their ID
		Map<Integer, Class<?extends Value>[]> classes = new HashMap<Integer, Class<? extends Value>[]>();
		Map<Integer, Operator> operators = new HashMap<Integer, Operator>();
		
		System.out.println("Got Plan: " + protoPlan);
		// First Build the strings we need to give to each process over the configuration
		// This are: 
		// 	- code of python-file(given as parameter pythonCode)
		// 	- The framework files, therefore we make a comma-separated list of the names
		//  	and a comma-separated list of the code inside of them
		String namesConfString = null;
		String contentsConfString = null;
		ArrayList<File> inputFiles= getListOfFrameworkInputFiles(frameworkPath);	    
		String names[] = new String[inputFiles.size()];
		String contents[] = new String[inputFiles.size()];
		// Read the names and the content of the framework files
		for (int i = 0; i < inputFiles.size(); i++){
			names[i] = inputFiles.get(i).getName();
			contents[i] = Files.toString(inputFiles.get(i), Charset.defaultCharset());
		}
		// Construct comma seperated Strings
		if(names != null && contents != null){
			StringBuilder sb = new StringBuilder();
			for(String n :names)
				sb.append(n + ProtobufPythonStreamer.CONFIG_PYTHON_FRAMEWORK_LIST_DELIMITER);
			sb.deleteCharAt(sb.length() - 1);
			namesConfString = sb.toString();
			sb = new StringBuilder();
			for(String c :contents)
				sb.append(c + ProtobufPythonStreamer.CONFIG_PYTHON_FRAMEWORK_LIST_DELIMITER);
			sb.deleteCharAt(sb.length() - 1);
			contentsConfString = sb.toString();
		}
		
		// Now we iterate over the vertices and construct a plan from them
		for(int i = 0; i < protoPlan.getVerticesCount(); i++){
			Vertex vertex = protoPlan.getVertices(i);
			Map<String, String> params = new HashMap<String, String>();
			VertexType type = vertex.getType(); 
			Operator tmpOperator;
			int vID = vertex.getID();
			
			Operator[] input1 = getInputOperators(vertex.getInputs1List(), operators);
			Operator[] input2 = getInputOperators(vertex.getInputs2List(), operators);
			classes.put(vID, getOutputClasses(vertex));
			
			// Get additional params for the vertex
			for (int j = 0; j < vertex.getParamsCount(); j++){
				KeyValuePair param = vertex.getParams(j);
				params.put(param.getKey(), param.getValue());
			}
			
			// Parse class of key and indices if there are any
			// I do this here because we need to do it for many operators
			Class<? extends Value> keyClass = null;
			int keyInd1 = 0, keyInd2 = 0;		
			if(params.get(PARAM_KEY_INDEX_1) != null){
				keyInd1 = Integer.valueOf(params.get(PARAM_KEY_INDEX_1));
				keyClass = classes.get(vertex.getInputs1(0))[keyInd1];
			}
			if(params.get(PARAM_KEY_INDEX_2) != null){
				keyInd2 = Integer.valueOf(params.get(PARAM_KEY_INDEX_2));
			}
	
			// For each operator construct a object of the correct class and save it into the array 
			switch(type){
				case TextInputFormat:
					tmpOperator = new FileDataSource(new TextInputFormat(), 
						params.get(PARAM_FILE_PATH));
					break;
				case Map:
					PyMapFunction mapFunction = new PyMapFunction(scriptPath, connection, classes.get(vertex.getInputs1(0)), vID);
					tmpOperator = MapOperator.builder(mapFunction)
						.input(input1)
						.build();
					break;
				case Reduce:
					PyReduceFunction reduceFunction = new PyReduceFunction(scriptPath, connection, classes.get(vertex.getInputs1(0)), vID);
					tmpOperator = ReduceOperator.builder(reduceFunction, (Class<? extends Key>) keyClass, keyInd1)
							.input(input1)
							.build();
					break;
					
				case Join:
					PyJoinFunction joinFunction = new PyJoinFunction(scriptPath, connection, classes.get(vertex.getInputs1(0)), classes.get(vertex.getInputs2(1)), vID);			
					tmpOperator = JoinOperator.builder(joinFunction,  (Class<? extends Key>)keyClass, keyInd1, keyInd2)
							.input1(input1)
							.input2(input2)
							.build();
					break;

				case Cross:
					PyCrossFunction crossFunction = new PyCrossFunction(scriptPath, connection, classes.get(vertex.getInputs1(0)), classes.get(vertex.getInputs2(1)), vID);
					tmpOperator = CrossOperator.builder(crossFunction)
							.input1(input1)
							.input2(input2)
							.build();
					break;
					
				case CoGroup:
					PyCoGroupFunction coGroup = new PyCoGroupFunction(scriptPath, connection, classes.get(vertex.getInputs1(0)), classes.get(vertex.getInputs2(1)), vID);
					tmpOperator = CoGroupOperator.builder(coGroup,  (Class<? extends Key>)keyClass, keyInd1, keyInd2)
							.input1(input1)
							.input2(input2)
							.build();
					break;
				case CsvOutputFormat:
					FileDataSink out = new FileDataSink(new CsvOutputFormat(), 
							params.get(PARAM_FILE_PATH), Arrays.asList(input1));
					CsvOutputFormat.configureRecordFormat(out)
						.recordDelimiter(params.get(PARAM_RECORD_DELIMITER).charAt(0))
						.fieldDelimiter(params.get(PARAM_FIELD_DELIMITER).charAt(0));
					String[] listIndices = params.get(PARAM_INDEX_LIST).split(",");
					for(String sInd : listIndices){
						int iInd = Integer.valueOf(sInd);
						CsvOutputFormat.configureRecordFormat(out)
							.field(classes.get(vertex.getInputs1(0))[iInd], iInd);
					}
					tmpOperator = out;
					break;
				default:
					throw new RuntimeException("Not implemented Vertex/Operatortype");
			}
			tmpOperator.setParameter(ProtobufPythonStreamer.CONFIG_PYTHON_FILE, pythonCode);
			tmpOperator.setParameter(ProtobufPythonStreamer.CONFIG_PYTHON_FRAMEWORK_CONTENTS, contentsConfString);
			tmpOperator.setParameter(ProtobufPythonStreamer.CONFIG_PYTHON_FRAMEWORK_NAMES, namesConfString);
			operators.put(vID, tmpOperator);
		}

		// Setup a List of DataSinks for the plan
		List<GenericDataSink> dataSinks = new ArrayList<GenericDataSink>();
		for(Integer dataSinkID : protoPlan.getDataSinkIDsList()){
			dataSinks.add((GenericDataSink)operators.get(dataSinkID));
		}
		Plan result = new Plan(dataSinks, "Python-language-binding");
		return result;
	}

	private Operator[] getInputOperators(List<Integer> inputs, Map<Integer, Operator> operators) {
		Operator[] result = new Operator[inputs.size()];
		for(int i = 0; i < inputs.size(); i++){
			result[i] = operators.get(inputs.get(i));
		}
		return result;
	}

	private ArrayList<File> getListOfFrameworkInputFiles(String frameworkPath) {
		File folder = new File(frameworkPath);
		ArrayList<File> resultFiles = new ArrayList<File>();
		File[] filesInFolder = folder.listFiles();
		
		// Go recursively through the framework folder and add all files to the result if they are .py files
		for (int i = 0; i < filesInFolder.length; i++) {
			String fileName = filesInFolder[i].getName();
			if (filesInFolder[i].isFile() && fileName.endsWith(".py")) {
				resultFiles.add(filesInFolder[i]);
			} else if (filesInFolder[i].isDirectory()) {
				// Remove this again, it's only here because wordcount example is in the same directory currently
				if(!fileName.equals("wordcountexample")){
					resultFiles.addAll(getListOfFrameworkInputFiles(frameworkPath + fileName + "/"));
				}
		    }
		}
		
		return resultFiles;
	}
}
