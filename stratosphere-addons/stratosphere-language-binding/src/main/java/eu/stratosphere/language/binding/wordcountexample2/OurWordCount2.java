package eu.stratosphere.language.binding.wordcountexample2;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import com.google.protobuf.CodedOutputStream;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.language.binding.protos.KeyValueProtos.KeyValuePair;
import eu.stratosphere.language.binding.protos.KeyValueProtos.KeyValueStream;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

/*
 * 13.01 FH: Looking at performance of reducer:
 *  - How much are we slower?
 *  	For the first 100 lines of hamlet.txt 
 *  	the python reducer takes around 24s instead of the usual wc-java-reducer 0.5s
 *  	measured in RegularPactTask in the run() call in invoke() method
 *   
 *  - Do we need flush after each sent kvp ?
 *  	Also works without, but makes no performance difference.
 *  - Do we need flush after each sent (-1) ?
 *  	Yes :D
 *  
 *  - Is bufferedInput/Output stream better or worse?
 *  	Also doesn't seem to make a difference
 *  	
 * 	- How much is inet-socket costing against normal socket?
 * 		We need extra library for this, so i just skip it first
 *  - How much is socket costing against pipe(let's try std.in std.out) ?
 *  	The pipe is around 0.65 s - so the sockets really are the bottleneck
 *  	and pipes would solve the problem
 *  
 *  - Can we handle the python buffer more intelligent?
 *  	Communication time is the real bottleneck,
 *  	the time we are waiting from the 
 *  	socket.receive call in the python process is around 22 sec
 *  	while socket.send and the whole rest 
 *  	(include encoding/decoding were less then a second)
 */

/*
 * 15.01 FH: 
 * 1. Building Datatype for Stratosphere Record
 * 		Done and prototype is running.
 * 		Also introduced a fix length size
 * 		-> But now it's much slower -.-
 * 
 * - Why is it so much slower?
 * 		Test how much difference it makes with varInt sizes
 * 		nearly nothing... but java code is cleaner that way, so i let it inside the code for now :D
 * 
 * 2. Making the Record conversion somehow dependent on a list of classes and look if it is working
 * 		done with static list so far
 * 
 * 3. Build iterator in Python for reducer?
 * - Change Python interface to Python tuples -> ended with buggy code 
 */

/*	16.01 FH: Fix buggy python code from yesterday
 * 	-> Fixed it, reducer now running with python utils class
 */
		
public class OurWordCount2 implements Program, ProgramDescription {

	public static class OrigTokenizeLine extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void map(Record record, Collector<Record> collector) {
			// get the first field (as type StringValue) from the record
			String line = record.getField(0, StringValue.class).getValue();

			//System.out.println("line: " + line);
			// normalize the line
			line = line.replaceAll("\\W+", " ").toLowerCase();
			
			// tokenize the line
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				// we emit a (word, 1) pair 
				collector.collect(new Record(new StringValue(word), new IntValue(1)));
			}
		}
	}
	
	@ConstantFields(0)
	public static class OrigCountWords extends ReduceFunction implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
			Record element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
				int cnt = element.getField(1, IntValue.class).getValue();
				sum += cnt;
			}

			element.setField(1, new IntValue(sum));
			out.collect(element);
		}
		
		@Override
		public void combine(Iterator<Record> records, Collector<Record> out) throws Exception {
			// the logic is the same as in the reduce function, so simply call the reduce method
			reduce(records, out);
		}
	}
	/**
	 * Converts a Record containing one string in to multiple string/integer
	 * pairs. The string is tokenized by whitespaces. For each token a new
	 * record is emitted, where the token is the first field and an Integer(1)
	 * is the second field.
	 */
	public static class TokenizeLine extends MapFunction implements
			Serializable {
		
		
		private static final long serialVersionUID = 1L;
		public static final int PORT = 8080;
		private Socket pythonSocket;
		private ServerSocket sSocket;
		private Process pythonProcess;
		private InputStream inStream;
		private OutputStream outStream;
		
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			System.out.println("[Mapper] Open is called");
			String[] env = { "PYTHONPATH=src/main/java/eu/stratosphere/language/binding/protos" };
			this.pythonProcess = Runtime
					.getRuntime()
					.exec("python src/main/java/eu/stratosphere/language/binding/wordcountexample/Mapper.py "
							+ PORT, env);

			ServerSocket server = new ServerSocket(PORT);
			this.sSocket = server;
			System.out.println("[Mapper] Waiting for connection on port " + PORT);
			Socket socket = server.accept();
			this.pythonSocket = socket;
			this.inStream = pythonSocket.getInputStream();
			this.outStream = pythonSocket.getOutputStream();
			System.out.println("[Mapper] OutputThread: got connection on port " + PORT);
		}

		@Override
		public void close() throws Exception {
			OutputStream out = pythonSocket.getOutputStream();
			final CodedOutputStream cout = CodedOutputStream.newInstance(out);
			cout.writeRawVarint32(-1);
			cout.flush();

			sSocket.close();
			inStream.close();
			outStream.close();
			System.out.println("[Mapper]close is called");
			super.close();
		}

		@Override
		public void map(Record record, Collector<Record> collector) {
			// get the first field (as type StringValue) from the record
			String line = record.getField(0, StringValue.class).getValue();

			try {
				KeyValuePair.Builder kvBuilder = KeyValuePair.newBuilder();
				kvBuilder.setKey(line);
				kvBuilder.setValue(0);
				KeyValuePair kv = kvBuilder.build();

//				System.out.println("Sending to Python: " + line + " size: "
//						+ line.length());

				kv.writeDelimitedTo(outStream);
				outStream.flush();

				KeyValueStream kvs = KeyValueStream
						.parseDelimitedFrom(inStream);

				for (int i = 0; i < kvs.getRecordCount(); i++) {
					collector.collect(new Record(new StringValue(kvs.getRecord(
							i).getKey()), new IntValue(kvs.getRecord(i)
							.getValue())));
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	/**
	 * Sums up the counts for a certain given key. The counts are assumed to be
	 * at position <code>1</code> in the record. The other fields are not
	 * modified.
	 */
//	@Combinable
	@ConstantFields(0)
	public static class CountWords extends ReduceFunction implements
			Serializable {
		private static final long serialVersionUID = 1L;
		
		// Some so far hardcoded values, which will be replaced in the future
		// Just made them static that they are written in italic :D
		public static final int PORT = 8081;
		public static final String REDUCESCRIPTPATH = "python src/main/java/eu/stratosphere/language/binding/wordcountexample2/reducer2.py";
		public static final String[] ENV = { "PYTHONPATH=src/main/java/eu/stratosphere/language/binding/protos" };
		public static final Class<?>[] REDUCECLASSES = { StringValue.class, IntValue.class };
		
		private Socket pythonSocket;
		private ServerSocket sSocket;
		private Process pythonProcess;
		private InputStream inStream;
		private OutputStream outStream;
		private PythonReducer2 reducer;
		//private BufferedReader err;
		
		private final boolean useSockets = false;
		@SuppressWarnings("unchecked")
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			
			System.out.println("[Reducer] Open is called reducer");
			this.pythonProcess = Runtime.getRuntime().exec(REDUCESCRIPTPATH, ENV);

			//err = new BufferedReader(new InputStreamReader(pythonProcess.getErrorStream()));
			if(useSockets){
				ServerSocket server = new ServerSocket(PORT);
				this.sSocket = server;
				System.out.println("[Reducer] Waiting for connection on port " + PORT);
				Socket socket = server.accept();
				System.out.println("[Reducer] Got connection on port " + PORT);
				this.pythonSocket = socket;
				this.inStream = pythonSocket.getInputStream();
				this.outStream = pythonSocket.getOutputStream();
			}else{
				this.outStream = pythonProcess.getOutputStream(); // this thing is buffered with 8k
				this.inStream = pythonProcess.getInputStream(); // this thing is buffered with 8k
				System.out.println("[Reducer] Opened process with stdin/stdout communication");
			}
		
			ArrayList<Class<?extends Value>> reduceClasses = new ArrayList<Class<? extends Value>>();
			for(int i = 0; i < REDUCECLASSES.length; i++){
				reduceClasses.add((Class<? extends Value>) REDUCECLASSES[i]);
			}

			this.reducer = new PythonReducer2(inStream, outStream, reduceClasses);
			System.out.println("[Reducer] initialized");
		}

		@Override
		public void close() throws Exception {
			System.out.println("[Reducer] Close is called");
			
			/*String line;
			while ((line = err.readLine()) != null) {
				System.err.println("Python Error: "+line);
			}*/
			
			//final CodedOutputStream cout = CodedOutputStream.newInstance(outStream);
			//cout.writeRawVarint32(-2);
			
			pythonProcess.destroy();
			if(useSockets){
				sSocket.close();
			}
			inStream.close();
			outStream.close();
			super.close();
		}

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			reducer.reduce(records, out);
			
		}

		@Override
		public void combine(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			// the logic is the same as in the reduce function, so simply call
			// the reduce method
			reduce(records, out);
		}
	}

	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String output = (args.length > 2 ? args[2] : "");

		FileDataSource source = new FileDataSource(new TextInputFormat(),
				dataInput, "Input Lines");
		MapOperator mapper = MapOperator.builder(new OrigTokenizeLine())
				.input(source).name("Tokenize Lines")
				.build();
		
		reducerInput = new ArrayList<Class<? extends Value>>();
		reducerInput.add(StringValue.class);
		reducerInput.add(IntValue.class);
		
		ReduceOperator reducer = ReduceOperator
				.builder(CountWords.class, StringValue.class, 0).input(mapper)
				.name("Count Words").build();
		FileDataSink out = new FileDataSink(new CsvOutputFormat(), output,
				reducer, "Word Counts");
		CsvOutputFormat.configureRecordFormat(out).recordDelimiter('\n')
				.fieldDelimiter(' ').field(StringValue.class, 0)
				.field(IntValue.class, 1);

		Plan plan = new Plan(out, "WordCount Example");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [input] [output]";
	}

	public static ArrayList<Class<? extends Value>> reducerInput;
	
	public static void main(String[] args) throws Exception {
		OurWordCount2 wc = new OurWordCount2();

		if (args.length < 3) {
			System.err.println(wc.getDescription());
			System.exit(1);
		}

		Plan plan = wc.getPlan(args);

		long ts1 = System.currentTimeMillis();
		LocalExecutor.execute(plan);
		System.out.println("Needed: " + (System.currentTimeMillis() - ts1)/1000.0f + "s");
	}

}
