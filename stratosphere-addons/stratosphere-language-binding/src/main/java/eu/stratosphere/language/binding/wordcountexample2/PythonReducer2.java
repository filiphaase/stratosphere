package eu.stratosphere.language.binding.wordcountexample2;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.language.binding.protos.StratosphereRecordProtoBuffers.ProtoRecordSize;
import eu.stratosphere.language.binding.protos.StratosphereRecordProtoBuffers.ProtoStratosphereRecord;
import eu.stratosphere.language.binding.protos.StratosphereRecordProtoBuffers.ProtoStratosphereRecord.ProtoValue;
import eu.stratosphere.language.binding.protos.StratosphereRecordProtoBuffers.ProtoStratosphereRecord.ProtoValueType;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public class PythonReducer2 {

	private InputStream inStream;
	private OutputStream outStream;
	private ArrayList<Class<? extends Value>> inputClasses;
	
	public PythonReducer2(InputStream inStream, OutputStream outStream) {
		this.inStream = inStream;
		this.outStream = outStream;
	}
	
	
	public PythonReducer2(InputStream inStream, OutputStream outStream,
			ArrayList<Class<? extends Value>> reducerInput) {
		this(inStream, outStream);
		this.inputClasses = reducerInput;
	}

	public void reduce(Iterator<Record> records, Collector<Record> collectorOut)
			throws Exception {
		// For each record write the size as sizeType and then the kvp
		while (records.hasNext()) {
			Record element = records.next();
			ProtoStratosphereRecord psr = getProtoStratosphereRecord(element);
			sendSize(psr.getSerializedSize());
			psr.writeTo(outStream);
			outStream.flush();
		}

		sendSize(-1);
		outStream.flush();

		ProtoStratosphereRecord psr = ProtoStratosphereRecord.parseDelimitedFrom(inStream);
		
		Record r = getRecord(psr);
		collectorOut.collect(r);
		
		// We don't need this for the reducer so far
		/*int size = getSize();
		if(size != -1){
			byte[] b = new byte[size];
			inStream.read(b, 0, size);
			result = ProtoStratosphereRecord.parseFrom(b);
			
			collectorOut.collect(getRecord(result));
		}*/
	}

	private void sendSize(int serializedSize) throws Exception{
		ProtoRecordSize size = ProtoRecordSize.newBuilder()
				.setValue(serializedSize)
				.build();
		size.writeTo(outStream);
		outStream.flush();
	}
	
	private int getSize() throws Exception{
		byte[] b = new byte[5];
		inStream.read(b, 0, 5);
		
		ProtoRecordSize prs = ProtoRecordSize.parseFrom(b);
		return prs.getValue();
	}

	private ProtoStratosphereRecord getProtoStratosphereRecord(Record r) {
		ProtoStratosphereRecord.Builder psrb=  ProtoStratosphereRecord.newBuilder();
		
		for(int i = 0; i < inputClasses.size(); i++){
			Class<? extends Value> inputClass = inputClasses.get(i);
			if(inputClass == StringValue.class){
				psrb.addValues(ProtoValue.newBuilder()
						.setValueType(ProtoValueType.StringValue)
						.setStringVal(r.getField(i, StringValue.class).getValue()));
			}else if(inputClass == IntValue.class){
				psrb.addValues(ProtoValue.newBuilder()
						.setValueType(ProtoValueType.IntegerValue32)
						.setInt32Val(r.getField(i, IntValue.class).getValue()));	
							
			}
		}
		return psrb.build();
	}
	
	private Record getRecord(ProtoStratosphereRecord psr) {
		Record result = new Record(psr.getValuesCount());
		
		for( int i = 0; i < psr.getValuesCount(); i++){
			ProtoValue protoVal = psr.getValues(i);
			Value val = null;
			switch(protoVal.getValueType()){
				case StringValue: 
					val = new StringValue(protoVal.getStringVal());
					break;
				case IntegerValue32: 
					val = new IntValue(protoVal.getInt32Val());
					break;
				default:
					throw new RuntimeException("Any not implemented ProtoValType received");
			}
			result.setField(i, val);
		}
		
		return result;
	}
}
