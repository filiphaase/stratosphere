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
package eu.stratosphere.api.common.typeutils.comparator.base;

import eu.stratosphere.api.common.typeutils.ComparatorTestBase;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.CharComparator;
import eu.stratosphere.api.common.typeutils.base.CharSerializer;
import java.util.Random;

public class CharComparatorTest extends ComparatorTestBase<Character> {

	@Override
	protected TypeComparator<Character> createComparator(boolean ascending) {
		return new CharComparator(ascending);
	}

	@Override
	protected TypeSerializer<Character> createSerializer() {
		return new CharSerializer();
	}

	@Override
	protected Character[] getSortedTestData() {
		Random rnd = new Random(874597969123412338L);
		int rndChar = rnd.nextInt(Character.MAX_VALUE);
		if(rndChar<0){
			rndChar=-rndChar;
		}
		if(rndChar==(int)Character.MIN_VALUE){
			rndChar+=2;
		}
		if(rndChar==(int)Character.MAX_VALUE){
			rndChar-=2;
		}
		return new Character[]{
			Character.MIN_VALUE,
			(char)rndChar,
			(char)(rndChar+1),
			Character.MAX_VALUE			
		};
	}
}
