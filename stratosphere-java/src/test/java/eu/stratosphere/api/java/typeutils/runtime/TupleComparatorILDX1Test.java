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
package eu.stratosphere.api.java.typeutils.runtime;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.DoubleComparator;
import eu.stratosphere.api.common.typeutils.base.DoubleSerializer;
import eu.stratosphere.api.common.typeutils.base.IntComparator;
import eu.stratosphere.api.common.typeutils.base.IntSerializer;
import eu.stratosphere.api.common.typeutils.base.LongComparator;
import eu.stratosphere.api.common.typeutils.base.LongSerializer;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.typeutils.runtime.tuple.base.TupleComparatorTestBase;

public class TupleComparatorILDX1Test extends TupleComparatorTestBase<Tuple3> {

	Tuple3[] dataISD = new Tuple3[]{
		new Tuple3<Integer, Long, Double>(4, new Long(4), 20.0),
		new Tuple3<Integer, Long, Double>(4, new Long(4), 23.2),
		new Tuple3<Integer, Long, Double>(4, new Long(9), 20.0),
		new Tuple3<Integer, Long, Double>(4, new Long(9), 24.0),
		new Tuple3<Integer, Long, Double>(4, new Long(19), 23.2),
		new Tuple3<Integer, Long, Double>(4, new Long(19), 24.0),
		new Tuple3<Integer, Long, Double>(4, new Long(24), 20.0),
		new Tuple3<Integer, Long, Double>(4, new Long(24), 23.2)
	};

	@Override
	protected TupleComparator<Tuple3> createComparator(boolean ascending) {
		return new TupleComparator<Tuple3>(
				new int[]{1},
				new TypeComparator[]{
					new IntComparator(ascending),
					new LongComparator(ascending),
					new DoubleComparator(ascending)
				});
	}

	@Override
	protected TupleSerializer<Tuple3> createSerializer() {
		return new TupleSerializer<Tuple3>(
				Tuple3.class,
				new TypeSerializer[]{
					new IntSerializer(),
					new LongSerializer(),
					new DoubleSerializer()});
	}

	@Override
	protected int getNormalizedKeyLength() {
		return 12;
	}

	@Override
	protected Class<Tuple3> getTypeClass() {
		return Tuple3.class;
	}

	@Override
	protected Tuple3[] getSortedTestData() {
		return dataISD;
	}

}
