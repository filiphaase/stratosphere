package eu.stratosphere.language.binding.java;

import java.io.File;
import java.nio.charset.Charset;

import com.google.common.io.Files;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.language.binding.java.Streaming.ConnectionType;
import eu.stratosphere.language.binding.java.Streaming.ProtobufPlanStreamer;

public class PyStratosphereExecutor implements Program{

	public static void main(String args[]) throws Exception{
		
		PyStratosphereExecutor pse = new PyStratosphereExecutor();
		long ts1 = System.currentTimeMillis();
		LocalExecutor.execute(pse.getPlan(args));
		System.out.println("Needed: " + (System.currentTimeMillis() - ts1)/1000.0f + "s");
	}

	@Override
	public Plan getPlan(String... args) {
		try{
			// Read arguments
			String call = "Call with PyStratosphereExecutor [PythonFilePath] [PythonFrameworkPath]";
			if(args.length != 2){
				System.out.println(call);
				System.exit(1);
			}
			String filePath = args[0];
			String frameworkPath = args[1];
			// Python part of framework only supports STDPIPES completely right now
			ConnectionType connectionType = ConnectionType.STDPIPES;
			
			// Build the 
			String[] env = { ("PYTHONPATH=" + frameworkPath + ":" + frameworkPath + "protos/")};
			String pythonCode = Files.toString(new File(filePath), Charset.defaultCharset());

			// Use The Plan Streamer to execute the python program and get back the stratosphere plan
			ProtobufPlanStreamer streamer = new ProtobufPlanStreamer(filePath, connectionType, env);
			streamer.open();
			streamer.streamSignalGetPlan();
			Plan plan = streamer.receivePlan(filePath, pythonCode, frameworkPath);
			streamer.close();
			return plan;
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
