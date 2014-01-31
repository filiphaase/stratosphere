package eu.stratosphere.language.binding.java.Streaming;

import eu.stratosphere.api.common.Plan;

public class ProtobufPlanStreamer extends ProtobufPythonStreamer {

	private PlanReceiver planReceiver;
	private PlanSender planSender;

	public ProtobufPlanStreamer(String pythonFilePath,
			ConnectionType connectionType) {
		super(pythonFilePath, connectionType, null);
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

	public Plan receivePlan(String scriptPath) throws Exception{
		return planReceiver.receivePlan(scriptPath, connectionType);	
	}
}
