package eu.stratosphere.language.binding.java.Streaming;

import java.util.Iterator;
import java.util.List;

import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public class ProtobufTupleStreamer extends ProtobufPythonStreamer {

	private RecordReceiver receiver;
	private RecordSender sender;
	
	public ProtobufTupleStreamer(String pythonFilePath,
			ConnectionType connectionType, List<Class<?extends Value>> classes) {
		super(pythonFilePath, connectionType, classes);
	}
	
	public void open() throws Exception{
		super.open();
		sender = new RecordSender(outputStream, inputRecordClasses);
		receiver = new RecordReceiver(inputStream);
	}

	/**
	 * Sends a single record to the sub-process (without and signal afterwards) 
	 * and directly starts to receive records afterwards
	 */
	public void streamSingleRecord(Record record, Collector<Record> collector) throws Exception {
        sender.sendSingleRecord(record);
        receiver.receive(collector);
	}
	
	/**
	 * Sends multiple records to the sub-process. After all are send a signal is send to the sub-process
	 * and then it starts to receive records
	 */
    public void streamMultipleRecords(Iterator<Record> records, Collector<Record> collector) throws Exception {
        sender.sendAllRecords(records);
        receiver.receive(collector);
    }
    
    public void sendID(int id) throws Exception{
    	sender.sendSize(id);
    }
}
