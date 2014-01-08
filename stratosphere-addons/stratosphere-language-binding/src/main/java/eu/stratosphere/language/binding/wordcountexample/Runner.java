package eu.stratosphere.language.binding.wordcountexample;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import eu.stratosphere.language.binding.protos.KeyValueProtos.KeyValuePair;
import eu.stratosphere.language.binding.protos.KeyValueProtos.KeyValueStream;

/* TODO-List:
 * 	1) Multiple Map outputs
 * 	2) Simple WordCount-Mapper-Job
 *  3) Simple WordCount-Reducer-Job
 *  4) Erste Stratosphere Integration - Test
 *  
 *  5) Thread Reevaluation
 *  6) PACT-RECORD Abbildung in Protocol buffers
 *  7) "Python Interface Design"
 *  8) PACT-Operators schreiben
 * 
 */
public class Runner {
	
	public static final int PORT = 8080;
	
	private static void printKeyValueStream(KeyValueStream kvs){
		for(int i = 0; i < kvs.getRecordCount(); i++){
			KeyValuePair kvp = kvs.getRecord(i);
			System.out.println("Got kvp " + i + ": (" + kvp.getKey() + ":" + kvp.getValue() + ")");
		}
	}
	private static KeyValuePair generateKeyValue() {
		KeyValuePair.Builder kvpb = KeyValuePair.newBuilder();
		Random r = new Random();

		kvpb.setKey("k-" + ((Integer)r.nextInt(1000000000)).toString()
			+ "-" + ((Integer)r.nextInt(1000000000)).toString()
			+ "-" + ((Integer)r.nextInt(1000000000)).toString()
			+ "-" + ((Integer)r.nextInt(1000000000)).toString()
			+ "-" + ((Integer)r.nextInt(1000000000)).toString()
			+ "-" + ((Integer)r.nextInt(1000000000)).toString()
			+ "-" + ((Integer)r.nextInt(1000000000)).toString()
			+ "-" + ((Integer)r.nextInt(1000000000)).toString()
			+ "-" + ((Integer)r.nextInt(1000000000)).toString()
			+ "-" + ((Integer)r.nextInt(1000000000)).toString()
			+ "-" + ((Integer)r.nextInt(1000000000)).toString()
			+ "-" + ((Integer)r.nextInt(1000000000)).toString()
			+ "-" + ((Integer)r.nextInt(1000000000)).toString()
			);
		kvpb.setValue("v-" + r.nextInt(1000000000));
		
		return kvpb.build();
	}
	
	
	public static void main(String args[]) throws Exception{
		
		String[] env =  {"PYTHONPATH=src/main/java/eu/stratosphere/language/binding/protos/"};
		Process p = Runtime.getRuntime().exec("python src/main/java/eu/stratosphere/language/binding/"
				+ "wordcountexample/runner.py", env);
		BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
		BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        
	    ServerSocket server = new ServerSocket(PORT);
	    System.out.println("Waiting for connection on port " + PORT);
	    Socket socket = server.accept();
	    System.out.println("OutputThread: got connection on port " + PORT);

        OutputStream out = socket.getOutputStream();
	    final CodedOutputStream cout = CodedOutputStream.newInstance(out);
	    
	    InputStream in = socket.getInputStream();
	    final CodedInputStream cin = CodedInputStream.newInstance(in);
	            
        System.out.println("Setting up");
   
        for(int i=0; i < 1; i++){
    		try{
        	// For each kvp write the size as int and then the kvp
	        KeyValuePair kvp = generateKeyValue();
	        int size = kvp.getSerializedSize();
			/* 
			 * Maximum size of a key value pair should be:
			 * 2^64, because the Varint which is used to send the size, can't take more
			 * See: https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.io.coded_stream?hl=en-US&csw=1
			 */
	        	System.out.println("Wrote size: " + size);    	
	        	kvp.writeDelimitedTo(out);
	        	out.flush();
	        	System.out.println("Wrote kvp: (" + kvp.getKey() + ":" + kvp.getValue() + ")");
	        
	        	KeyValueStream kvs = KeyValueStream.parseDelimitedFrom(in);
	        	printKeyValueStream(kvs);
	        	
    		}catch( Exception e){
    			e.printStackTrace();
    		}
	    }
	    System.out.println("Cleaning up");
        cout.writeRawVarint32(-1);
        cout.flush();
        
		
        // Some Printing
        String line;
        while ((line = input.readLine()) != null) {
        	System.err.println("Python: '"+line);
		}
        while ((line = err.readLine()) != null) {
        	System.err.println("Python Error: "+line);
		}
	}

}
