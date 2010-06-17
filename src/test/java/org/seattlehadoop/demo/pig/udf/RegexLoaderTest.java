package org.seattlehadoop.demo.pig.udf;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.junit.Test;
import org.seattlehadoop.demo.pig.udf.EmptyTuple;
import org.seattlehadoop.demo.pig.udf.RegexLoader;

public class RegexLoaderTest {

	@Test
	public void testRegexFails() throws IOException {
		RegexLoader loader = new RegexLoader("hello (.+)");
		String twoLines = "this line does not match\nhello world";
		InputStream in = new ByteArrayInputStream(twoLines.getBytes());
		loader.bindTo(null, new BufferedPositionedInputStream(in), 0, twoLines.length());
		Tuple tuple = loader.getNext();
		assertEquals(EmptyTuple.class, tuple.getClass());
		tuple = loader.getNext();
		assertEquals(1, tuple.size());
		assertEquals("world", (String) (tuple.get(0)));
		assertNull(loader.getNext());
	}

	@Test
	public void testMultipleRegexParts() throws IOException {
		RegexLoader loader = new RegexLoader("firstName: (\\S+), lastName: (\\S+)");
		String twoLines = "firstName: A, lastName: B\nfirstName: Tarquin, lastName: Fintim";
		InputStream in = new ByteArrayInputStream(twoLines.getBytes());
		loader.bindTo(null, new BufferedPositionedInputStream(in), 0, twoLines.length());
		Tuple tuple = loader.getNext();
		assertEquals("A", (String) tuple.get(0));
		assertEquals("B", (String) tuple.get(1));
		assertEquals(2, tuple.size());
		tuple = loader.getNext();
		assertEquals("Tarquin", (String) tuple.get(0));
		assertEquals("Fintim", (String) tuple.get(1));
		assertEquals(2, tuple.size());
		assertNull(loader.getNext());
	}
}
