/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.example.java.wordcount;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.*;
import eu.stratosphere.util.Collector;


@SuppressWarnings("serial")
public class WordCountPojo {
	
	public static class WC{
		public String s;
		public int i;
		
		public WC(String s, int i){
			this.s = s;
			this.i = i;
		}
		
		@Override
		public int hashCode() {
			return s.hashCode();
		}
	}
	
	public static final class Tokenizer extends FlatMapFunction<String, WC> {

		@Override
		public void flatMap(String value, Collector<WC> out) {
			String[] tokens = value.toLowerCase().split("\\W");
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new WC(token, 1));
				}
			}
		}
	}
	
	public static final class ReduceCountFuntion extends ReduceFunction<WC> {

		@Override
		public WC reduce(WC v1, WC v2) throws Exception {
			return new WC(v1.s, v1.i + v2.i);
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: WordCount <input path> <result path>");
			return;
		}
		
		final String input = args[0];
		final String output = args[1];
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);
		
		DataSet<String> text = env.readTextFile(input);
		
		DataSet<WC> words = text.flatMap(new Tokenizer());
		
		DataSet<WC> result = words.reduce(new ReduceCountFuntion());
		
		result.writeAsText(output);
		env.execute("Word Count");
	}
}
