package eu.stratosphere.language.binding.java.Streaming;

import java.io.Serializable;

public enum ConnectionType implements Serializable{
	STDPIPES, 
	PIPES, 
	SOCKETS
}
