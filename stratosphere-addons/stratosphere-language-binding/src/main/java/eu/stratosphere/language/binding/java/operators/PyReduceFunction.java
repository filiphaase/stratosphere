package eu.stratosphere.language.binding.java.operators;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.language.binding.java.Streaming.ConnectionType;
import eu.stratosphere.language.binding.java.Streaming.ProtobufTupleStreamer;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public class PyReduceFunction extends ReduceFunction implements Serializable{

		private static final long serialVersionUID = 1L;
		
		private transient ProtobufTupleStreamer streamer;
		private ConnectionType connectionType;
		private List<Class<? extends Value>> classes;
		private int id;
		
		public PyReduceFunction(String scriptPath, ConnectionType connectionType, List<Class<? extends Value>> classes, int id){
			this.connectionType = connectionType;
			this.classes = classes;
			this.id = id;
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			streamer = new ProtobufTupleStreamer(parameters, connectionType, classes);
			streamer.open();
			streamer.sendID(id);
		}

		@Override
		public void close() throws Exception {
			streamer.close();
			super.close();
		}

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			streamer.streamMultipleRecords(records, out);
		}
}
