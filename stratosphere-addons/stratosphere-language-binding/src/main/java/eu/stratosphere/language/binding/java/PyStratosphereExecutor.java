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
		
		// TODO: Over commandlne (bash script)
		String frameworkPath = "/home/filip/workspaceDIMA/stratosphere/stratosphere-addons/stratosphere-language-binding/src/main/python/eu/stratosphere/language/binding/";
		String[] env = { ("PYTHONPATH=" + frameworkPath + ":" + frameworkPath + "protos/")};
		String pythonCode = Files.toString(new File(path), Charset.defaultCharset());

		System.out.println("Custruct Streamer");
		// Use The Plan Streamer to execute the python program and get back the stratosphere plan
		ProtobufPlanStreamer streamer = new ProtobufPlanStreamer(path, connectionType, env);
		System.out.println("-> constructed");
		streamer.open();
		System.out.println("Openened streamer");
		streamer.streamSignalGetPlan();
		System.out.println("Streamed Signal");
		Plan plan = streamer.receivePlan(path, pythonCode, frameworkPath);
		streamer.close();
		
		// Execute it
		long ts1 = System.currentTimeMillis();
		LocalExecutor.execute(plan);
		System.out.println("Needed: " + (System.currentTimeMillis() - ts1)/1000.0f + "s");
	}

	@Override
	public Plan getPlan(String... args) {
		try{
			System.out.println("Called getPlan()");
			for(String arg : args)
				System.out.println("Arg: " + arg);
			
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
			
			// TODO: Over commandlne (bash script)
			String frameworkPath = "/home/filip/workspaceDIMA/stratosphere/stratosphere-addons/stratosphere-language-binding/src/main/python/eu/stratosphere/language/binding/";
			String[] env = { ("PYTHONPATH=" + frameworkPath + ":" + frameworkPath + "protos/")};
			String pythonCode = Files.toString(new File(path), Charset.defaultCharset());

			System.out.println("Custruct Streamer");
			// Use The Plan Streamer to execute the python program and get back the stratosphere plan
			ProtobufPlanStreamer streamer = new ProtobufPlanStreamer(path, connectionType, env);
			System.out.println("-> constructed");
			streamer.open();
			System.out.println("Openened streamer");
			streamer.streamSignalGetPlan();
			System.out.println("Streamed Signal");
			Plan plan = streamer.receivePlan(path, pythonCode, frameworkPath);
			streamer.close();
			System.out.println("Done with getPlan()");
			return plan;
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
