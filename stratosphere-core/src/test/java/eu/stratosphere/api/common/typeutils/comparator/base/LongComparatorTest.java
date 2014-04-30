package eu.stratosphere.api.common.typeutils.comparator.base;

import java.util.Random;

import eu.stratosphere.api.common.typeutils.ComparatorTestBase;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.LongComparator;
import eu.stratosphere.api.common.typeutils.base.LongSerializer;

public class LongComparatorTest extends ComparatorTestBase<Long> {

	@Override
	protected TypeComparator<Long> createComparator(boolean ascending) {
		return new LongComparator(ascending);
	}

	@Override
	protected TypeSerializer<Long> createSerializer() {
		return new LongSerializer();
	}

	@Override
	protected int getNormalizeKeyLength() {
		return 8;
	}

	// Don't know if we will need this function
	@Override
	protected Class<Long> getTypeClass() {
		return Long.class;
	}

	@Override
	protected Long[] getSortedTestData() {
		Random rnd = new Random(874597969123412338L);
		long rndLong = rnd.nextLong();
		// Should be between 1 and Maxvalue or -1 and MinValue to have it sorted
		if(rndLong >=-1 && rndLong <= 1 ){
			// shift by 2 if not between 1 and -1
			rndLong += 3;
		}
		
		return new Long[] {new Long(Long.MIN_VALUE), new Long(-rndLong), new Long(-1L), 
				new Long(0L), new Long(1L), new Long(rndLong), new Long(Long.MAX_VALUE)};
	}

}
