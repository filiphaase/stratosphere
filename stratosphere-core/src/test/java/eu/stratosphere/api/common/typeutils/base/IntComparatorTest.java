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
package eu.stratosphere.api.common.typeutils.base;

import eu.stratosphere.api.common.typeutils.ComparatorTestBase;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.IntComparator;
import eu.stratosphere.api.common.typeutils.base.IntSerializer;

import java.util.Random;

public class IntComparatorTest extends ComparatorTestBase<Integer> {

	@Override
	protected TypeComparator<Integer> createComparator(boolean ascending) {
		return new IntComparator(ascending);
	}

	@Override
	protected TypeSerializer<Integer> createSerializer() {
		return new IntSerializer();
	}

	@Override
	protected Integer[] getSortedTestData() {

		Random rnd = new Random(874597969123412338L);
		int rndInt = rnd.nextInt();
		if (rndInt < 0) {
			rndInt = -rndInt;
		}
		if (rndInt == Integer.MAX_VALUE) {
			rndInt -= 3;
		}
		if (rndInt <= 2) {
			rndInt += 3;
		}
		return new Integer[]{
			new Integer(Integer.MIN_VALUE),
			new Integer(-rndInt),
			new Integer(-1),
			new Integer(0),
			new Integer(1),
			new Integer(2),
			new Integer(rndInt),
			new Integer(Integer.MAX_VALUE)};
	}
}
