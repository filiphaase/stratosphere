package eu.stratosphere.language.binding.java;

import java.util.ArrayList;

import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public class Mapper extends AbstractOperator {
	
	public Mapper(String pythonFilePath, int port,
			ArrayList<Class<? extends Value>> outputRecordClasses,
			ConnectionType connectionType) {
		super(pythonFilePath, port, outputRecordClasses, connectionType);
	}
	
	public Mapper(String pythonFilePath,
			ArrayList<Class<? extends Value>> outputRecordClasses,
			ConnectionType connectionType) {
		super(pythonFilePath, outputRecordClasses, connectionType);
	}

	
	public void map(Record record, Collector<Record> collector) throws Exception {
		sender.sendSingleRecord(record);
		receiver.receive(collector);
	}
}
