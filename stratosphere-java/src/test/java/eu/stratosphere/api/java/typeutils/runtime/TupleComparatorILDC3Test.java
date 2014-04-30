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

public class TupleComparatorILDC3Test extends TupleComparatorTestBase<Tuple3> {

	Tuple3[] dataISD = new Tuple3[]{
		new Tuple3<Integer, Long, Double>(4, new Long(4), 20.0),
		new Tuple3<Integer, Long, Double>(5, new Long(1), 20.0),
		new Tuple3<Integer, Long, Double>(5, new Long(2), 20.0),
		new Tuple3<Integer, Long, Double>(5, new Long(10), 23.0),
		new Tuple3<Integer, Long, Double>(5, new Long(19), 24.0),
		new Tuple3<Integer, Long, Double>(5, new Long(20), 24.0),
		new Tuple3<Integer, Long, Double>(5, new Long(24), 25.0),
		new Tuple3<Integer, Long, Double>(5, new Long(25), 25.0)
	};

	@Override
	protected TupleComparator<Tuple3> createComparator(boolean ascending) {
		return new TupleComparator<Tuple3>(
				new int[]{2, 0, 1},
				new TypeComparator[]{
					new DoubleComparator(ascending),
					new IntComparator(ascending),
					new LongComparator(ascending)
				},
		new TypeSerializer[]{new IntSerializer()});
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
	protected Tuple3[] getSortedTestData() {
		return dataISD;
	}

}
