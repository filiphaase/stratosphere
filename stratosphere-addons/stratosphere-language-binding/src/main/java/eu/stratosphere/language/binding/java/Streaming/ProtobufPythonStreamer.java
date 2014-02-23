package eu.stratosphere.language.binding.java.Streaming;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.types.Value;

/**
 * General class for streaming records fromthe java-language binding framework to a subprocess.
 * Implements the whole logic for calling the subprocess(python so far)
 * and setting up the connection and implements an open() and close() function.
 * 
 * Currently one must use streamSingleRecord() for Map-Operator and streamMultipleRecord for Reduce-Operator
 */
public class ProtobufPythonStreamer{

	// Signal that all records of a single map/reduce/... call are sent 
	public static final int SIGNAL_SINGLE_CALL_DONE = -1;
	// Signal that all map/reduce/... calls for one Operator in the plan are finished
	// and that the sub-process can terminate
	public static final int SIGNAL_ALL_CALLS_DONE = -2;
	// Signal that plan should be sent back
	public static final int SIGNAL_GET_PLAN = -1;
	
	private static final String[] ENV = { "PYTHONPATH=src/main/python/eu/stratosphere/language/binding/protos/:src/main/python/eu/stratosphere/language/binding/" };
	private static final int PORT = 49562;
	private static final Log LOG = LogFactory.getLog(ProtobufPythonStreamer.class);
	
	private final String pythonFilePath;
	protected final ConnectionType connectionType;
	protected List<Class<?extends Value>> inputRecordClasses;
	protected List<Class<?extends Value>> secondInputRecordClasses = null;
	
	protected InputStream inputStream;
	protected OutputStream outputStream;
	private Process pythonProcess;
	private ServerSocket serverSocket;
	private BufferedReader err;
	
	public ProtobufPythonStreamer(String pythonFilePath, ConnectionType connectionType,
			List<Class<?extends Value>> classes){
		this.pythonFilePath = pythonFilePath;
		this.connectionType = connectionType;
		this.inputRecordClasses = classes;
	}
	
	/**
	 * Used for operators with two different "input-streams" like join/group/co-group
	 */
	public ProtobufPythonStreamer(String pythonFilePath, ConnectionType connectionType,
			List<Class<?extends Value>> classes1, List<Class<?extends Value>> classes2){
		this(pythonFilePath, connectionType, classes1);
		this.secondInputRecordClasses = classes2;
	}
	
	public void open() throws Exception{
		if(connectionType == ConnectionType.SOCKETS){
			pythonProcess = Runtime.getRuntime().exec("python " + pythonFilePath, ENV);
		}else{
			pythonProcess = Runtime.getRuntime().exec("python " + pythonFilePath + " " + PORT, ENV);
		}
		//err = new BufferedReader(new InputStreamReader(pythonProcess.getErrorStream()));
		LOG.debug("Proto-AbstractOperator - open() called");
		
		switch(connectionType){
		case STDPIPES:
			outputStream = pythonProcess.getOutputStream(); // this thing is buffered with 8k
			inputStream = pythonProcess.getInputStream(); // this thing is buffered with 8k
			LOG.debug("Proto-AbstractOperator - started connection via stdin/stdout");
			break;
		case SOCKETS:
			// Currently not in the python code
			serverSocket = new ServerSocket(PORT);
			Socket pythonSocket = serverSocket.accept();
			inputStream = pythonSocket.getInputStream();
			outputStream = pythonSocket.getOutputStream();
			LOG.debug("Proto-AbstractOperator - initialized connection over port " + PORT);
			break;
		default:
			throw new Exception("Currently not implemented connection type, use STDPIPES");
		}
	}
	
	public void close() throws Exception{
		LOG.debug("Proto-AbstractOperator - close() called");
		
		// Send signal to the python process that it is done
		//if(sender != null)
			//sender.sendSize(SIGNAL_ALL_CALLS_DONE);
		/*String line;
		while ((line = err.readLine()) != null) {
			LOG.error("Python Error: "+line);
			System.out.println("Python Error: "+line);
		}*/
		
		pythonProcess.destroy();
		if(connectionType == ConnectionType.SOCKETS){
			serverSocket.close();
		}
		inputStream.close();
		outputStream.close();
	}
}
