package eu.stratosphere.language.binding.java.operators;

import java.io.Serializable;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.language.binding.java.Streaming.ConnectionType;
import eu.stratosphere.language.binding.java.Streaming.ProtobufTupleStreamer;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public class PyMapFunction extends MapFunction 
	implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private transient ProtobufTupleStreamer streamer;
	private ConnectionType connectionType;
	private Class<? extends Value>[] classes;
	private int id;
	
	public PyMapFunction(String scriptPath, ConnectionType connectionType, Class<? extends Value>[] classes, int id){
		this.connectionType = connectionType;
		this.classes = classes;
		this.id = id;
	}
	
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
	public void map(Record record, Collector<Record> collector) throws Exception{
		streamer.streamSingleRecord(record, collector); 
	}
}
