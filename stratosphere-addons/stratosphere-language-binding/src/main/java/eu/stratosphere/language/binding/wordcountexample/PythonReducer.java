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
	
	public static final int PORT = 8080;
	
	private void printKeyValueStream(KeyValueStream kvs){
		for(int i = 0; i < kvs.getRecordCount(); i++){
			KeyValuePair kvp = kvs.getRecord(i);
			System.out.println("GETTING: Got kvp " + i + ": (" + kvp.getKey() + ":" + kvp.getValue() + ")");
		}
	}
	
	public void reduce(Iterator<Record> records, Collector<Record> collectorOut ) throws Exception{
		KeyValueStream kvs = getKeyValueStream(records);
		
		String[] env =  {"PYTHONPATH=src/main/java/eu/stratosphere/language/binding/protos/"};
		Process p = Runtime.getRuntime().exec("python src/main/java/eu/stratosphere/language/binding/"
				+ "wordcountexample/reducer.py", env);
		BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
		BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		KeyValueStream kvsResult = null;
		
	    ServerSocket server = new ServerSocket(PORT);
	    System.out.println("STARTUP: Waiting for connection on port " + PORT);
	    Socket socket = server.accept();
	    System.out.println("STARTUP: Got connection on port " + PORT);

        OutputStream out = socket.getOutputStream();
	    final CodedOutputStream cout = CodedOutputStream.newInstance(out);
	    
	    InputStream in = socket.getInputStream();
   
   		try{
			/* 
			 * Maximum size of a key value pair should be:
			 * 2^64, because the Varint which is used to send the size, can't take more
			 * See: https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.io.coded_stream?hl=en-US&csw=1
			 */
        	// For each kvp write the size as int and then the kvp
	        int size = kvs.getSerializedSize();
	        System.out.println("SENDING: Size: " + size);
	    	System.out.println("SENDING: Stream with " + kvs.getRecordCount() + " records");
	        kvs.writeDelimitedTo(out);
	        out.flush();
	        
	        kvsResult = KeyValueStream.parseDelimitedFrom(in);
	        	
   		}catch( Exception e){
   			e.printStackTrace();
   		}
   		
	    System.out.println("Cleaning up");
        cout.writeRawVarint32(-1);
        cout.flush();
        
        //  Python Output Some Printing
        String line;
        while ((line = input.readLine()) != null) {
        	System.err.println("Python: '"+line);
		}
        while ((line = err.readLine()) != null) {
        	System.err.println("Python Error: "+line);
		}
        
        if(kvsResult != null){
            printKeyValueStream(kvsResult);
        	
        	// Some Printing
    		for(int i = 0; i < kvsResult.getRecordCount(); i++){
    			KeyValuePair kvp = kvsResult.getRecord(i);
    			Record element = new Record(new StringValue(kvp.getKey()), 
    					new IntValue(kvp.getValue()));
    			collectorOut.collect(element);
    		}
        }else{
        	System.out.println("GETTING: Got kvsResult null, exiting now" );
        }
		
        System.out.println("################# DONE #################");
		server.close();
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
