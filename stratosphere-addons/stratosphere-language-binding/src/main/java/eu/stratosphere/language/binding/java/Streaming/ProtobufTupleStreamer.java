package eu.stratosphere.language.binding.java.Streaming;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public class ProtobufTupleStreamer extends ProtobufPythonStreamer {

	private RecordReceiver receiver;
	private RecordSender sender;
	
	// Constructor for tuple streamer with one record-class(Map,Reduce)
	public ProtobufTupleStreamer(Configuration conf,
			ConnectionType connectionType, List<Class<?extends Value>> classes) throws IOException{
		super(conf, connectionType, classes);
	}
	
	// Constructor for tuple streamer with two different record-classes (Join,Cross,CoGroup) 
	public ProtobufTupleStreamer(Configuration conf,
			ConnectionType connectionType,
			List<Class<? extends Value>> classes1,
			List<Class<? extends Value>> classes2) throws IOException{
		super(conf, connectionType, classes1, classes2);
	}

	public void open() throws Exception{
		super.open();
		if(secondInputRecordClasses ==null){
			sender = new RecordSender(outputStream, inputRecordClasses);
		}else{
			sender = new RecordSender(outputStream, inputRecordClasses, secondInputRecordClasses);
		}
		receiver = new RecordReceiver(inputStream);
	}

	/**
	 * Sends a single record to the sub-process (without and signal afterwards) 
	 * and directly starts to receive records afterwards 
	 * Used for MAP-Operator
	 */
	public void streamSingleRecord(Record record, Collector<Record> collector) throws Exception {
        sender.sendSingleRecord(record);
        receiver.receive(collector);
	}
	
	/**
	 * Sends multiple records to the sub-process. After all are send a signal is send to the sub-process
	 * and then it starts to receive records
	 * Used for REDUCE-Operator
	 */
    public void streamMultipleRecords(Iterator<Record> records, Collector<Record> collector) throws Exception {
        sender.sendAllRecords(records);
        receiver.receive(collector);
    }
    
	/**
	 * Sends two records to the sub-process (without a signal afterwards) 
	 * and directly starts to receive records afterwards, 
	 * Used for JOIN- and CROSS-Operator
	 */
	public void streamTwoRecord(Record record1, Record record2, Collector<Record> collector) throws Exception {
        sender.sendSingleRecord(record1, 0);
        sender.sendSingleRecord(record2, 1);
        receiver.receive(collector);
	}
    
    /**
     * Sends records from two different iterators to the subprocess
     * The protocol works as follows, after the subprocess is started:
     * 		1.) subprocess sends integer
     * 		2.) - if integer is -3 -> Send back record from iterator1
     * 			- if integer is -4 -> Send back record from iterator1
     * 			- if integer is SIGNAL_SINGLE_CALL_DONE -> Subprocess is done
     * 			- if integer is any other number the subprocess is sending back
     * 				a result tuple and the integer is the size of it
     */
	public void streamTwoIterators(Iterator<Record> records1,
			Iterator<Record> records2, Collector<Record> out) throws Exception{
		int i;
		while((i = receiver.getSize()) != SIGNAL_SINGLE_CALL_DONE){
			// Send next record from one of the iterators
			// -3 is signal for iterater1 , -4 signal for iterator2
			if( i == -3 || i == -4){
				Iterator<Record> iter = null;
				if( i == -4){
					iter = records1;
					i = 0;
				}else if ( i == -3){
					iter = records2;
					i = 1;
				}
				if(iter != null && iter.hasNext()){
					sender.sendSingleRecord(iter.next(), i);
				}else{
					// Send signal that this iterator is done
					sender.sendSize(SIGNAL_SINGLE_CALL_DONE);
				}
			}else{
				// Case that subprocess wants to send back a result tuple
				receiver.receiveSingleRecord(out, i);
			}
		}
		//sender.sendSize(SIGNAL_ALL_CALLS_DONE);
	}
	
    public void sendID(int id) throws Exception{
    	sender.sendSize(id);
    }

	public void sendDone() throws Exception {
		sender.sendSize(SIGNAL_ALL_CALLS_DONE);
	}
}
