package eu.stratosphere.language.binding.java;

import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public class Reducer extends AbstractOperator {

	public Reducer(String pythonFilePath, int port,
			ArrayList<Class<? extends Value>> outputRecordClasses,
			ConnectionType connectionType) {
		super(pythonFilePath, port, outputRecordClasses, connectionType);
	}
	
	public Reducer(String pythonFilePath,
			ArrayList<Class<? extends Value>> outputRecordClasses,
			ConnectionType connectionType) {
		super(pythonFilePath, outputRecordClasses, connectionType);
	}

	public void reduce(Iterator<Record> records, Collector<Record> collector) throws Exception {
		sender.sendAllRecords(records);
		receiver.receive(collector);
	}

}
