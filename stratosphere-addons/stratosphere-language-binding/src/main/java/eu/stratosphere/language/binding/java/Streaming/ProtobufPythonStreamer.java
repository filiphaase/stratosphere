package eu.stratosphere.language.binding.java.Streaming;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.configuration.Configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.io.Files;

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
	
	public final static String CONFIG_PYTHON_FILE = "param.pythonCode";
	public final static String CONFIG_PYTHON_FRAMEWORK_NAMES = "param.pythonFrameworkNames";
	public final static String CONFIG_PYTHON_FRAMEWORK_CONTENTS = "param.pythonFrameworkContents";
	public final static String CONFIG_PYTHON_FRAMEWORK_LIST_DELIMITER = Character.toString((char)8);
	
	private static final int PORT = 49562;
	private static final Log LOG = LogFactory.getLog(ProtobufPythonStreamer.class);
	
	private String[] environment;
	protected final ConnectionType connectionType;
	private String pythonFilePath;
	protected List<Class<?extends Value>> inputRecordClasses;
	protected List<Class<?extends Value>> secondInputRecordClasses = null;
	
	protected InputStream inputStream;
	protected OutputStream outputStream;
	private Process pythonProcess;
	private ServerSocket serverSocket;
	private BufferedReader err;
	
	//Used for things like plan streamer
	public ProtobufPythonStreamer(String pythonScript, ConnectionType connectionType, String[] env) throws IOException{
		this.pythonFilePath = pythonScript;
		this.connectionType = connectionType;
		this.inputRecordClasses = null;
		this.environment = env;
	}
	
	public ProtobufPythonStreamer(Configuration conf, ConnectionType connectionType,
			List<Class<?extends Value>> classes) throws IOException{
		this.loadPythonFilesIntoTmp(conf);
		this.connectionType = connectionType;
		this.inputRecordClasses = classes;
	}
	/**
	 * Used for operators with two different "input-streams" like join/group/co-group
	 */
	public ProtobufPythonStreamer(Configuration conf, ConnectionType connectionType,
			List<Class<?extends Value>> classes1, List<Class<?extends Value>> classes2) throws IOException{
		this(conf, connectionType, classes1);
		this.secondInputRecordClasses = classes2;
	}
	
	public void loadPythonFilesIntoTmp(Configuration conf) throws IOException{
		
		// Read and write the python file
		String pythonCode = conf.getString(ProtobufTupleStreamer.CONFIG_PYTHON_FILE, "");
		File temp = File.createTempFile("stratosphere-tmp-python-code", ".tmp.py");
		Files.append(pythonCode, temp, Charset.defaultCharset());
		this.pythonFilePath = temp.getAbsolutePath();
		System.out.println("New Path to tmpPythonFile: " + pythonFilePath);
		
		// Read and write the python files from the framework 
		File tempDir = Files.createTempDir();
		this.environment  = new String[]{"PYTHONPATH="+tempDir.getAbsolutePath()};
		String[] fileNames = conf.getString(CONFIG_PYTHON_FRAMEWORK_NAMES, "")
				.split(CONFIG_PYTHON_FRAMEWORK_LIST_DELIMITER);
		String[] contents = conf.getString(CONFIG_PYTHON_FRAMEWORK_CONTENTS, "")
				.split(CONFIG_PYTHON_FRAMEWORK_LIST_DELIMITER);
		
		for(int i = 0; i < fileNames.length; i++){
			String path = tempDir.getAbsolutePath() + "/" + fileNames[i];
			Files.append(contents[i], new File(path), Charset.defaultCharset());
			System.out.println("Written tmp file: " + path);
		}
	}
	
	public void open() throws Exception{
		System.out.println("------- Executing file path in ProtobufStreamer.open(): " + pythonFilePath);
//		System.out.println("-------> File content: " + Files.toString(new File(pythonFilePath), Charset.defaultCharset()));
		System.out.println("with env: " + Arrays.toString(environment));
		if(connectionType == ConnectionType.SOCKETS){
			pythonProcess = Runtime.getRuntime().exec("python " + pythonFilePath, environment);
		}else{
			pythonProcess = Runtime.getRuntime().exec("python " + pythonFilePath + " " + PORT, environment);
		}
		System.out.println("exec done");
		//err = new BufferedReader(new InputStreamReader(pythonProcess.getErrorStream()));
		//LOG.debug("Proto-AbstractOperator - open() called");
		System.out.println("Getting streams");
		switch(connectionType){
		case STDPIPES:
			System.out.println("Getting stdpipes");
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
		System.out.println("Close called");
		
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
