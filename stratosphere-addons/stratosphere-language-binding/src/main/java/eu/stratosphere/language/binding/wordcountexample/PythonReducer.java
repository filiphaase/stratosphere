package eu.stratosphere.language.binding.wordcountexample;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import com.google.protobuf.CodedOutputStream;

import eu.stratosphere.language.binding.protos.KeyValueProtos.KeyValuePair;
import eu.stratosphere.language.binding.protos.KeyValueProtos.KeyValueStream;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class PythonReducer {

	private InputStream inStream;
	private OutputStream outStream;

	public PythonReducer(InputStream inStream, OutputStream outStream) {
		this.inStream = inStream;
		this.outStream = outStream;
	}

	public void reduce(Iterator<Record> records, Collector<Record> collectorOut)
			throws Exception {
		KeyValuePair kvpResult = null;

		/*
		 * Maximum size of a key value pair should be: 2^64, because the Varint
		 * which is used to send the size, can't take more See:
		 * https://developers
		 * .google.com/protocol-buffers/docs/reference/cpp/google
		 * .protobuf.io.coded_stream?hl=en-US&csw=1
		 */
		
		// For each kvp write the size as int and then the kvp
		while (records.hasNext()) {
			Record element = records.next();
			KeyValuePair kvp = getKeyValuePair(element);
			kvp.writeDelimitedTo(outStream);
			outStream.flush();
		}

		final CodedOutputStream cout = CodedOutputStream.newInstance(outStream);
		cout.writeRawVarint32(-1);
		cout.flush();

		kvpResult = KeyValuePair.parseDelimitedFrom(inStream);
		System.out.println("[Reducer] Result for " + kvpResult.getKey() + " = "
				+ kvpResult.getValue());
		collectorOut.collect(getRecord(kvpResult));
	}

	private Record getRecord(KeyValuePair kvp) {
		return new Record(new StringValue(kvp.getKey()), new IntValue(
				kvp.getValue()));
	}

	private KeyValuePair getKeyValuePair(Record r) {
		return (KeyValuePair.newBuilder().setKey(
				r.getField(0, StringValue.class).getValue()).setValue(r
				.getField(1, IntValue.class).getValue())).build();
	}
}
