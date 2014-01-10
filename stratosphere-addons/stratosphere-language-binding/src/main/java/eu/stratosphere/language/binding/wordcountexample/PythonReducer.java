package eu.stratosphere.language.binding.wordcountexample;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import eu.stratosphere.language.binding.protos.KeyValueProtos.KeyValuePair;
import eu.stratosphere.language.binding.protos.KeyValueProtos.KeyValueStream;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class PythonReducer {
	
	private InputStream inStream;
	private OutputStream outStream;
	
	public PythonReducer(InputStream inStream, OutputStream outStream){
		this.inStream = inStream;
		this.outStream = outStream;
	}
	
	private void printKeyValueStream(KeyValueStream kvs){
		for(int i = 0; i < kvs.getRecordCount(); i++){
			KeyValuePair kvp = kvs.getRecord(i);
			System.out.println("GETTING: Got kvp " + i + ": (" + kvp.getKey() + ":" + kvp.getValue() + ")");
		}
	}
	
	public void reduce(Iterator<Record> records, Collector<Record> collectorOut ) throws Exception{
		KeyValueStream kvs = getKeyValueStream(records);
		KeyValueStream kvsResult = null;
		
   		try{
			/* 
			 * Maximum size of a key value pair should be:
			 * 2^64, because the Varint which is used to send the size, can't take more
			 * See: https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.io.coded_stream?hl=en-US&csw=1
			 */
        	// For each kvp write the size as int and then the kvp
	        int size = kvs.getSerializedSize();
	        //System.out.println("SENDING: Size: " + size);
	        //System.out.println("SENDING: Stream with " + kvs.getRecordCount() + " records");
	        kvs.writeDelimitedTo(outStream);
	        outStream.flush();
	        
	        kvsResult = KeyValueStream.parseDelimitedFrom(inStream);
	        	
   		}catch( Exception e){
   			e.printStackTrace();
   		}
   		
   		//System.out.println("Cleaning up");
	    //CodedOutputStream cout = CodedOutputStream.newInstance(outStream);
        //cout.writeRawVarint32(-1);
        //cout.flush();
        
        if(kvsResult != null){
        	//printKeyValueStream(kvsResult);
        	
        	// Some Printing
    		for(int i = 0; i < kvsResult.getRecordCount(); i++){
    			KeyValuePair kvp = kvsResult.getRecord(i);
    			Record element = new Record(new StringValue(kvp.getKey()), 
    					new IntValue(kvp.getValue()));
    			collectorOut.collect(element);
    		}
        }else{
        	//System.out.println("GETTING: Got kvsResult null, exiting now" );
        }
		
        //System.out.println("################# DONE #################");
	}

	private KeyValueStream getKeyValueStream(Iterator<Record> records) {
		KeyValueStream.Builder result = KeyValueStream.newBuilder();
		Record element = null;
		KeyValuePair.Builder tmpKVPBuilder = null;
		
		while(records.hasNext()){
			element = records.next();
			tmpKVPBuilder = KeyValuePair.newBuilder()
					.setKey(element.getField(0, StringValue.class).getValue())
					.setValue(element.getField(1, IntValue.class).getValue());
			result.addRecord(tmpKVPBuilder);
		}
		return result.build();
	}	
}
