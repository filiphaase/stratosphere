package eu.stratosphere.language.binding.java;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import eu.stratosphere.types.Value;

/**
 * General class for any operators of the java-language binding framework
 * Implements the whole logic for calling the subprocess(python so far)
 * and setting up the connection and implements an open() and close() function.
 * 
 * The operator which implements this abstract class only needs to write something
 * like a run() function where he uses the receiver and sender for streaming records
 * 
 * @author Filip Haase
 */
public abstract class AbstractOperator {

	public static final int SIGNAL_SINGLE_CALL_DONE = -1;
	public static final int SIGNAL_ALL_CALLS_DONE = -2;
	
	private static final String[] ENV = { "PYTHONPATH=src/main/java/eu/stratosphere/language/binding/protos/:src/main/java/eu/stratosphere/language/binding/python/" };
	private final String pythonFilePath;
	private final int port;
	private final ArrayList<Class<?extends Value>> inputRecordClasses;
	private final ConnectionType connectionType;
	
	private InputStream inputStream;
	private OutputStream outputStream;
	private Process pythonProcess;
	private ServerSocket serverSocket;
	private BufferedReader err;
	
	protected RecordReceiver receiver;
	protected RecordSender sender;
	
	public AbstractOperator(String pythonFilePath, int port, ArrayList<Class<?extends Value>> outputRecordClasses,
			ConnectionType connectionType){
		this.pythonFilePath = pythonFilePath;
		this.port = port;
		this.inputRecordClasses = outputRecordClasses;
		this.connectionType = connectionType;
	}
	
	public AbstractOperator(String pythonFilePath, ArrayList<Class<?extends Value>> outputRecordClasses,
			ConnectionType connectionType){
		this(pythonFilePath, -1, outputRecordClasses, connectionType);
	}
	
	public void open() throws Exception{
		if(port == -1){
			pythonProcess = Runtime.getRuntime().exec(pythonFilePath, ENV);
		}else{
			pythonProcess = Runtime.getRuntime().exec(pythonFilePath + " " + port, ENV);
		}
		err = new BufferedReader(new InputStreamReader(pythonProcess.getErrorStream()));
		System.out.println("Proto-AbstractOperator - open() called");
		
		switch(connectionType){
		case STDPIPES:
			outputStream = pythonProcess.getOutputStream(); // this thing is buffered with 8k
			inputStream = pythonProcess.getInputStream(); // this thing is buffered with 8k
			System.out.println("Proto-AbstractOperator - started connection via stdin/stdout");
			break;
		case SOCKETS:
			// Currently not in the python code
			serverSocket = new ServerSocket(port);
			Socket pythonSocket = serverSocket.accept();
			inputStream = pythonSocket.getInputStream();
			outputStream = pythonSocket.getOutputStream();
			System.out.println("Proto-AbstractOperator - initialized connection over port " + port);
			break;
		default:
			throw new Exception("Currently not implemented connection type, use STDPIPES");
		}
		
		sender = new RecordSender(outputStream, inputRecordClasses);
		receiver = new RecordReceiver(inputStream);
	}
	
	public void close() throws Exception{
		System.out.println("Proto-AbstractOperator - close() called");
		// Send signal to the python process that it is done
		sender.sendSize(SIGNAL_ALL_CALLS_DONE);
		
		String line;
		while ((line = err.readLine()) != null) {
			System.err.println("Python Error: "+line);
		}
		
		pythonProcess.destroy();
		if(connectionType == ConnectionType.SOCKETS){
			serverSocket.close();
		}
		inputStream.close();
		outputStream.close();
	}
}
