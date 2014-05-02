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
package eu.stratosphere.api.common.typeutils;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Abstract test base for serializers.
 * @param <T>
 */
public abstract class ComparatorTestBase<T> {

	protected abstract TypeComparator<T> createComparator(boolean ascending);

	protected abstract TypeSerializer<T> createSerializer();

	protected abstract int getNormalizedKeyLength();

	protected abstract Class<T> getTypeClass();

	protected abstract T[] getSortedTestData();

	// --------------------------------------------------------------------------------------------
	@Test
	public void testGetLength() {
		try {
			TypeComparator<T> comparator = getComparator(true);
			assertEquals(getNormalizedKeyLength(), comparator.getNormalizeKeyLen());
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	@Test
	public void testEqualsAscending() {
		testEquals(true);
	}

	@Test
	public void testEqualsDescending() {
		testEquals(false);
	}

	protected void testEquals(boolean ascending) {
		try {
			// Just setup two identical output/inputViews and go over their data to see if compare works
			TestOutputView out1 = new TestOutputView();
			writeSortedData(getSortedData(), out1);

			TestOutputView out2 = new TestOutputView();
			writeSortedData(getSortedData(), out2);

			TestInputView in1 = out1.getInputView();
			TestInputView in2 = out2.getInputView();

			// Now use comparator and compar
			TypeComparator<T> comparator = getComparator(ascending);
			T[] data = getSortedData();
			for (T e : data) {
				assertTrue(comparator.compare(in1, in2) == 0);
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception in test: " + e.getMessage());
		}
	}

	@Test
	public void testGreaterAscending() {
		testGreatSmallAscDesc(true, true);
	}

	@Test
	public void testGreaterDescending() {
		testGreatSmallAscDesc(false, true);
	}

	@Test
	public void testSmallerAscending() {
		testGreatSmallAscDesc(true, false);
	}

	@Test
	public void testSmallerDescending() {
		testGreatSmallAscDesc(false, false);
	}

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
			for (T h : high) {
				//workaround for generic array creation
				T[] selectedH = Arrays.copyOf(low, low.length);
				for (int x = 0; x < selectedH.length; x++) {
					selectedH[x] = h;
				}

				//create high inputView
				out2 = new TestOutputView();
				writeSortedData(selectedH, out2);
				in2 = out2.getInputView();

				//reset low InputView
				out1 = new TestOutputView();
				writeSortedData(low, out1);
				in1 = out1.getInputView();
				for (T l : low) {
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

	// --------------------------------------------------------------------------------------------
	//returntype used for quick escapes
	protected String deepEquals(String message, T should, T is) {
		if (should.getClass().isArray()) {
			//is the type check really necessary? Object one worked fine for int/bool/etc.
			if (should instanceof long[]) {
				assertArrayEquals(message, (long[]) should, (long[]) is);
				return null;
			}
			if (should instanceof short[]) {
				assertArrayEquals(message, (short[]) should, (short[]) is);
				return null;
			}
			if (should instanceof int[]) {
				assertArrayEquals(message, (int[]) should, (int[]) is);
				return null;
			}
			if (should instanceof boolean[]) {
				for (int x = 0; x < ((boolean[]) should).length; x++) {
					assertTrue(((boolean[]) should)[x] == ((boolean[]) is)[x]);
				}
				return null;
			}
			if (should instanceof float[]) {
				assertArrayEquals(message, (float[]) should, (float[]) is, (float) 0.0001);
				return null;
			}
			if (should instanceof double[]) {
				assertArrayEquals(message, (double[]) should, (double[]) is, 0.0001);
				return null;
			}
			if (should instanceof byte[]) {
				assertArrayEquals(message, (byte[]) should, (byte[]) is);
				return null;
			}

			assertArrayEquals(message, (Object[]) should, (Object[]) is);
			return null;

		} else {
			assertEquals(message, should, is);
			return null;
		}
	}

	// --------------------------------------------------------------------------------------------
	protected TypeComparator<T> getComparator(boolean ascending) {
		TypeComparator<T> comparator = createComparator(ascending);
		if (comparator == null) {
			throw new RuntimeException("Test case corrupt. Returns null as comparator.");
		}
		return comparator;
	}

	protected T[] getSortedData() {
		T[] data = getSortedTestData();
		if (data.length % 2 != 0) {
			throw new RuntimeException("Test case corrupt. Data must ocntain an even number of elements.");
		}
		if (data == null) {
			throw new RuntimeException("Test case corrupt. Returns null as test data.");
		}
		return data;
	}

	protected TypeSerializer<T> getSerializer() {
		TypeSerializer<T> serializer = createSerializer();
		if (serializer == null) {
			throw new RuntimeException("Test case corrupt. Returns null as serializer.");
		}
		return serializer;
	}

	protected void writeSortedData(T[] data, TestOutputView out) throws IOException {
		TypeSerializer<T> serializer = getSerializer();

		// Write data into a outputView
		for (T value : data) {
			serializer.serialize(value, out);
		}

		// This are the same tests like in the serializer
		// Just look if the data is really there after serialization, before testing comparator on it
		TestInputView in = out.getInputView();
		for (T value : data) {
			assertTrue("No data available during deserialization.", in.available() > 0);

			T deserialized = serializer.deserialize(serializer.createInstance(), in);
			deepEquals("Deserialized value if wrong.", value, deserialized);
		}
	}

	// --------------------------------------------------------------------------------------------
	protected static final class TestOutputView extends DataOutputStream implements DataOutputView {

		public TestOutputView() {
			super(new ByteArrayOutputStream(4096));
		}

		public TestInputView getInputView() {
			ByteArrayOutputStream baos = (ByteArrayOutputStream) out;
			return new TestInputView(baos.toByteArray());
		}

		@Override
		public void skipBytesToWrite(int numBytes) throws IOException {
			for (int i = 0; i < numBytes; i++) {
				write(0);
			}
		}

		@Override
		public void write(DataInputView source, int numBytes) throws IOException {
			byte[] buffer = new byte[numBytes];
			source.readFully(buffer);
			write(buffer);
		}
	}

	protected static final class TestInputView extends DataInputStream implements DataInputView {

		public TestInputView(byte[] data) {
			super(new ByteArrayInputStream(data));
		}

		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
			while (numBytes > 0) {
				int skipped = skipBytes(numBytes);
				numBytes -= skipped;
			}
		}
	}
}
