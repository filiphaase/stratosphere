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
	
	public ProtobufTupleStreamer(Configuration conf,
			ConnectionType connectionType, List<Class<?extends Value>> classes) throws IOException{
		super(conf, connectionType, classes);
	}
	
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
	 * and directly starts to receive records afterwards used for MAP
	 */
	public void streamSingleRecord(Record record, Collector<Record> collector) throws Exception {
        sender.sendSingleRecord(record);
        receiver.receive(collector);
	}
	
	/**
	 * Sends two records to the sub-process (without and signal afterwards) 
	 * and directly starts to receive records afterwards, used for JOIN
	 */
	public void streamTwoRecord(Record record1, Record record2, Collector<Record> collector) throws Exception {
        sender.sendSingleRecord(record1, 0);
        sender.sendSingleRecord(record2, 1);
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

	public void streamTwoIterators(Iterator<Record> records1,
			Iterator<Record> records2, Collector<Record> out) throws Exception{
		//boolean done[] = { false, false};
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
				System.out.println("Iter: " + iter);
				if(iter != null && iter.hasNext()){
					System.out.println("Sending record");
					sender.sendSingleRecord(iter.next(), i);
				}else{
					System.out.println("Sending signal single call done");
					// Send signal that this iterator is done
					sender.sendSize(SIGNAL_SINGLE_CALL_DONE);
				}
			}else{
				System.out.println("Receiving record with size: " + i);
				receiver.receiveSingleRecord(out, i);
			}
		}
		System.out.println("CoGroup DONE!");
		//sender.sendSize(SIGNAL_ALL_CALLS_DONE);
	}

	public void sendDone() throws Exception {
		sender.sendSize(SIGNAL_ALL_CALLS_DONE);
	}
}
