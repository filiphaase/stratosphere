package eu.stratosphere.language.binding.java.Streaming;

import java.io.OutputStream;

import eu.stratosphere.language.binding.protos.StratosphereRecordProtoBuffers.ProtoRecordSize;

public class PlanSender {

	private OutputStream outStream;
	
	public PlanSender(OutputStream outStream) {
		this.outStream = outStream;
	}

	public void sendID(int id) throws Exception{
		ProtoRecordSize size = ProtoRecordSize.newBuilder()
				.setValue(id)
				.build();
		size.writeTo(outStream);
		outStream.flush();
	}
	
}
