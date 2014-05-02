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
package eu.stratosphere.api.java.typeutils.runtime.tuple.base;

import eu.stratosphere.api.common.typeutils.ComparatorTestBase;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.runtime.TupleComparator;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class TupleComparatorTestBase<T extends Tuple> extends ComparatorTestBase<T> {

	@Override
	protected String deepEquals(String message, T should, T is) {
		for (int x = 0; x < should.getArity(); x++) {
			assertEquals(should.getField(x), is.getField(x));
		}
		return null;
	}

	@Override
	protected void testEquals(boolean ascending) {
		try {
			// Just setup two identical output/inputViews and go over their data to see if compare works
			TestOutputView out1;
			TestOutputView out2;
			TestInputView in1;
			TestInputView in2;
			
			// Now use comparator and compar
			TypeComparator<T> comparator = getComparator(ascending);
			T[] data1 = getSortedData();
			T[] data2 = getSortedData();
			for (int x = 0; x < data1.length; x++) {

				out2 = new TestOutputView();
				writeSortedData(Arrays.copyOfRange(data1, x, x + 1), out2);
				in2 = out2.getInputView();

				out1 = new TestOutputView();
				writeSortedData(Arrays.copyOfRange(data1, x, x + 1), out1);
				in1 = out1.getInputView();

				assertTrue(comparator.compare(in1, in2) == 0);
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	@Override
	protected void testGreatSmallAscDesc(boolean ascending, boolean greater) {
		try {
			//split data into low and high part
			T[] data = getSortedData();
			T[] low = Arrays.copyOfRange(data, 0, data.length / 2);
			T[] high = Arrays.copyOfRange(data, data.length / 2, data.length);

			TypeComparator<T> comparator = getComparator(ascending);
			TestOutputView out1;
			TestOutputView out2;
			TestInputView in1;
			TestInputView in2;

			//compares every element in high with every element in low
			for (int h = 0; h < high.length; h++) {
				for (int l = 0; l < low.length - 1; l++) {
					out2 = new TestOutputView();
					writeSortedData(Arrays.copyOfRange(high, h, h + 1), out2);
					in2 = out2.getInputView();

					out1 = new TestOutputView();
					writeSortedData(Arrays.copyOfRange(low, l, l + 1), out1);
					in1 = out1.getInputView();

					if (greater && ascending) {
						assertTrue(comparator.compare(in1, in2) < 0);
					}
					if (greater && !ascending) {
						assertTrue(comparator.compare(in1, in2) > 0);
					}
					if (!greater && ascending) {
						assertTrue(comparator.compare(in2, in1) > 0);
					}
					if (!greater && !ascending) {
						assertTrue(comparator.compare(in2, in1) < 0);
					}
				}
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	protected abstract int[] getNormalizedKeyLengths();

	@Override
	protected abstract TupleComparator<T> createComparator(boolean ascending);

	@Override
	protected abstract TupleSerializer<T> createSerializer();

}
