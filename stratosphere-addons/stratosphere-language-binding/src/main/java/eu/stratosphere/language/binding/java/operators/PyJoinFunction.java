package eu.stratosphere.language.binding.java.operators;

import java.io.Serializable;

import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.language.binding.java.Streaming.ConnectionType;
import eu.stratosphere.language.binding.java.Streaming.ProtobufTupleStreamer;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public class PyJoinFunction extends JoinFunction implements Serializable{ 
		private static final long serialVersionUID = 1L;
		
		private transient ProtobufTupleStreamer streamer;
		private ConnectionType connectionType;
		private Class<? extends Value>[] classes1;
		private Class<? extends Value>[] classes2;
		private int id;
		
		public PyJoinFunction(String scriptPath, ConnectionType connectionType, 
				Class<? extends Value>[] classes1, Class<? extends Value>[] classes2,
				int id){
			this.connectionType = connectionType;
			this.classes1 = classes1;
			this.classes2 = classes2;
			this.id = id;
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			streamer = new ProtobufTupleStreamer(parameters, connectionType, classes1, classes2);
			streamer.open();
			streamer.sendID(id);
		}

		@Override
		public void close() throws Exception {
			streamer.close();
			super.close();
		}

		@Override
		public void join(Record value1, Record value2, Collector<Record> out)
				throws Exception {
			streamer.streamTwoRecord(value1, value2, out);
		}
}
