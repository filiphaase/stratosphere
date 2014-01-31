package eu.stratosphere.language.binding.java;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.language.binding.java.Streaming.ConnectionType;
import eu.stratosphere.language.binding.java.Streaming.ProtobufPlanStreamer;

public class PyStratosphereExecutor {

	public static void main(String args[]) throws Exception{
		String call = "Call with PyStratosphereExecutor [PythonPath] [STDPIPES/SOCKETS]";
		if(args.length < 2){
			System.out.println(call);
			System.exit(1);
		}
		String path = args[0];
		ConnectionType connectionType = null;
		if(args[1].equals("STDPIPES")){
			connectionType = ConnectionType.STDPIPES;
		}else if(args[1].equals("SOCKETS")){
			connectionType = ConnectionType.SOCKETS;
		}else{
			System.out.println(call);
			System.exit(1);			
		}

		// Use The Plan Streamer to execute the python program and get back the stratosphere plan
		ProtobufPlanStreamer streamer = new ProtobufPlanStreamer(path, connectionType);
		streamer.open();
		streamer.streamSignalGetPlan();
		Plan plan = streamer.receivePlan(path);
		streamer.close();
		
		// Execute it
		long ts1 = System.currentTimeMillis();
		LocalExecutor.execute(plan);
		System.out.println("Needed: " + (System.currentTimeMillis() - ts1)/1000.0f + "s");
	}
}
