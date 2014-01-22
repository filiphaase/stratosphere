package eu.stratosphere.language.binding.java;

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

public class RecordSender {

	private OutputStream outStream;
	private ArrayList<Class<? extends Value>> inputClasses;
	
	public RecordSender(OutputStream outStream,
			ArrayList<Class<? extends Value>> inputClasses) {
		this.inputClasses = inputClasses;
		this.outStream = outStream;
	}
	
	public void sendAllRecords(Iterator<Record> records) throws Exception{
		while (records.hasNext()) {
			Record element = records.next();
			sendSingleRecord(element);
		}
		sendSize(AbstractOperator.SIGNAL_SINGLE_CALL_DONE);
		outStream.flush();
	}

	public void sendSingleRecord(Record record) throws Exception{
		ProtoStratosphereRecord psr = getProtoStratosphereRecord(record);
		sendSize(psr.getSerializedSize());
		psr.writeTo(outStream);
		outStream.flush();
	}
	
	public void sendSize(int serializedSize) throws Exception{
		ProtoRecordSize size = ProtoRecordSize.newBuilder()
				.setValue(serializedSize)
				.build();
		size.writeTo(outStream);
		outStream.flush();
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
}
