/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task;

import java.util.Hashtable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.functions.GenericGroupReduce;
import eu.stratosphere.api.common.functions.GenericReduce;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * Reduce task which is executed by a Nephele task manager. The task has a
 * single input and one or multiple outputs. It is provided with a ReduceFunction
 * implementation.
 * <p>
 * The ReduceTask creates a iterator over all records from its input. The iterator returns all records grouped by their
 * key. The iterator is handed to the <code>reduce()</code> method of the ReduceFunction.
 * 
 * @see GenericGroupReduce
 */
public class HashReduceDriver<T> implements PactDriver<GenericReduce<T>, T> {
	
	private static final Log LOG = LogFactory.getLog(HashReduceDriver.class);

	private PactTaskContext<GenericReduce<T>, T> taskContext;
	
	private MutableObjectIterator<T> input;

	private TypeSerializer<T> serializer;

	private TypeComparator<T> comparator;
	
	private Hashtable<Integer, T> hashtable;
	
	private volatile boolean running;

	// ------------------------------------------------------------------------

	@Override
	public void setup(PactTaskContext<GenericReduce<T>, T> context) {
		this.taskContext = context;
		this.running = true;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<GenericReduce<T>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericReduce<T>> clazz = (Class<GenericReduce<T>>) (Class<?>) GenericReduce.class;
		return clazz;
	}

	@Override
	public boolean requiresComparatorOnInput() {
		return true;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void prepare() throws Exception {
		System.out.println("Called prepare()");
		TaskConfig config = this.taskContext.getTaskConfig();
		if (config.getDriverStrategy() != DriverStrategy.HASH_REDUCE) {
			throw new Exception("Unrecognized driver strategy for Reduce driver: " + config.getDriverStrategy().name());
		}
		this.serializer = this.taskContext.getInputSerializer(0);
		this.comparator = this.taskContext.getInputComparator(0);
		this.input = this.taskContext.getInput(0);
		this.hashtable = new Hashtable<Integer, T>();
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("Reducer preprocessing done. Running Reducer code."));
		}

		System.out.println("In HashReduceDriver run()");
		System.out.println("");
		
		final GenericReduce<T> stub = this.taskContext.getStub();
		T record = this.serializer.createInstance();
		while (this.running && ((record = input.next(record)) != null)) {
			Integer hashvalue = new Integer(this.comparator.hash(record));
			// If the Value is already in the hashtable reduce it with the new value, and write them back
			if(hashtable.containsKey(hashvalue)){
				record = stub.reduce(this.hashtable.get(hashvalue), record);
				this.hashtable.put(hashvalue, record);
			// Otherwise just put the value in the hashtable
			}else{
				this.hashtable.put(hashvalue, record);
			}
			record = this.taskContext.<T>getInputSerializer(0).createInstance();
		}
		// Afterward all records are reduced, collect the output
		final Collector<T> output = this.taskContext.getOutputCollector();
		for ( Integer e : this.hashtable.keySet()){
			output.collect(this.hashtable.get(e));
		}
	}

	@Override
	public void cleanup() {}

	@Override
	public void cancel() {
		this.running = false;
	}
}