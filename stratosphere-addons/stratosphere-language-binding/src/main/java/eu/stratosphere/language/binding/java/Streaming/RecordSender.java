package eu.stratosphere.language.binding.java.Streaming;

import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.language.binding.protos.StratosphereRecordProtoBuffers.ProtoRecordSize;
import eu.stratosphere.language.binding.protos.StratosphereRecordProtoBuffers.ProtoStratosphereRecord;
import eu.stratosphere.language.binding.protos.StratosphereRecordProtoBuffers.ProtoStratosphereRecord.ProtoValue;
import eu.stratosphere.language.binding.protos.StratosphereRecordProtoBuffers.ProtoStratosphereRecord.ProtoValueType;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

/**
 * Class for serializing and sending java-records to a subprocess.
 * sendAllRecords() must be used for sending multiple records
 * sendSingleRecord() if an operator call only has a single record to sent(Map-Operator) 
 */
public class RecordSender {

	private OutputStream outStream;
	private Class<? extends Value>[] inputClasses1;
	private Class<? extends Value>[] inputClasses2;
	
	public RecordSender(OutputStream outStream,
			Class<? extends Value>[] inputClasses) {
		this.inputClasses1 = inputClasses;
		this.outStream = outStream;
	}
	
	public RecordSender(OutputStream outputStream,
			Class<? extends Value>[] inputRecordClasses,
			Class<? extends Value>[] secondInputRecordClasses) {
		this.inputClasses1 = inputRecordClasses;
		this.inputClasses2 = secondInputRecordClasses;
		this.outStream = outputStream;
	}

	public void sendAllRecords(Iterator<Record> records) throws Exception{
		while (records.hasNext()) {
			Record element = records.next();
			sendSingleRecord(element);
		}
		// After all records for a single map/reduce... call are sent,
		// send the signal to tell the sub-process that no records will be sent anymore
		sendSize(ProtobufPythonStreamer.SIGNAL_SINGLE_CALL_DONE);
		outStream.flush();
	}

	/**
	 * Send a single record to the sub-process and do not send any signal afterwards.
	 * This is used by the map-operator. Since we know there that we always only sent a single
	 * record and not multiple records in a mapper, we don't need an extra signal for telling the
	 * python process that this were all records.
	 */
	public void sendSingleRecord(Record record) throws Exception{
		sendSingleRecord(record, this.inputClasses1);
	}
	
	// Mhh... using int here is kinda ugly, change it
	public void sendSingleRecord(Record record, int classNumber) throws Exception{
		if(classNumber == 0){
			sendSingleRecord(record, this.inputClasses1);
		}else{
			sendSingleRecord(record, this.inputClasses2);
		}
	}
	
	public void sendSingleRecord(Record record, Class<? extends Value>[] classes) throws Exception{
		//System.out.println("in Send single record classes: " + classes.get(0));
		ProtoStratosphereRecord psr = getProtoStratosphereRecord(record, classes);
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
	
	
	/**
	 * Builds a protobuf-record from the java-record to send it to the subprocess
	 * Curently one int and strings are supported
	 */
	private ProtoStratosphereRecord getProtoStratosphereRecord(Record r, Class<? extends Value>[] inputClasses) throws Exception {
		ProtoStratosphereRecord.Builder psrb=  ProtoStratosphereRecord.newBuilder();
		
		for(int i = 0; i < inputClasses.length; i++){
			Class<? extends Value> inputClass = inputClasses[i];
			if(inputClass == StringValue.class){
				psrb.addValues(ProtoValue.newBuilder()
						.setValueType(ProtoValueType.StringValue)
						.setStringVal(r.getField(i, StringValue.class).getValue()));
			}else if(inputClass == IntValue.class){
				psrb.addValues(ProtoValue.newBuilder()
						.setValueType(ProtoValueType.IntegerValue32)
						.setInt32Val(r.getField(i, IntValue.class).getValue()));	
							
			}else{
				throw new Exception("Currently unimplemented value-type in the Record"); 
			}
		}
		return psrb.build();
	}
}
