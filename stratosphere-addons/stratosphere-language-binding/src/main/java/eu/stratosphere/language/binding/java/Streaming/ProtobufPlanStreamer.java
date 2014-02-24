package eu.stratosphere.language.binding.java.Streaming;

import java.io.IOException;

import eu.stratosphere.api.common.Plan;

public class ProtobufPlanStreamer extends ProtobufPythonStreamer {

	private PlanReceiver planReceiver;
	private PlanSender planSender;

	public ProtobufPlanStreamer(String pythonFilePath,
			ConnectionType connectionType) throws IOException{
		super(pythonFilePath, connectionType);
	}
	
	public void open() throws Exception{
		super.open();
		planSender = new PlanSender(outputStream);
		planReceiver = new PlanReceiver(inputStream);
	}

    public void streamSignalGetPlan() throws Exception{
    	streamID(ProtobufPythonStreamer.SIGNAL_GET_PLAN);
    }
    
    public void streamID(int id) throws Exception{
    	planSender.sendID(id);
    }

	public Plan receivePlan(String scriptPath, String pythonCode) throws Exception{
		return planReceiver.receivePlan(scriptPath, connectionType, pythonCode);	
	}
}
