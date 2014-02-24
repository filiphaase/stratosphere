package eu.stratosphere.language.binding.java.operators;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.language.binding.java.Streaming.ConnectionType;
import eu.stratosphere.language.binding.java.Streaming.ProtobufTupleStreamer;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public class PyJoinFunction extends JoinFunction implements Serializable{ 
		private static final long serialVersionUID = 1L;
		
		private transient ProtobufTupleStreamer streamer;
		private String scriptPath;
		private ConnectionType connectionType;
		private List<Class<? extends Value>> classes1;
		private List<Class<? extends Value>> classes2;
		private int id;
		
		public PyJoinFunction(String scriptPath, ConnectionType connectionType, 
				List<Class<? extends Value>> classes1, List<Class<? extends Value>> classes2,
				int id){
			this.scriptPath = scriptPath;
			this.connectionType = connectionType;
			this.classes1 = classes1;
			this.classes2 = classes2;
			this.id = id;
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			System.out.println("Open JoinFunction");
			streamer = new ProtobufTupleStreamer(parameters, connectionType, classes1, classes2);
			System.out.println("Open JoinFunction1");
			streamer.open();
			System.out.println("Open JoinFunction2");
			streamer.sendID(id);
			System.out.println("Open JoinFunction3");
		}

		@Override
		public void close() throws Exception {
			System.out.println("Close JoinFunction");
			streamer.close();
			super.close();
		}

		@Override
		public void join(Record value1, Record value2, Collector<Record> out)
				throws Exception {
			System.out.println("join JoinFunction called" + value1 + " / " + value2);
			streamer.streamTwoRecord(value1, value2, out);
		}
}
