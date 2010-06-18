package org.seattlehadoop.demo.pig.udf;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.junit.Test;
import org.seattlehadoop.demo.pig.udf.QuerystringLoader;

public class QuerystringLoaderTest {

	@Test
	public void testParseURI() throws IOException {
		String url = "http://localhost/foo?utm_source=feedburner&utm_medium=feed&utm_campaign=Feed:+kotaku/full+%28Kotaku%29&ignore=me1&ignore=me2";
		QuerystringLoader loader = new QuerystringLoader("utm_medium", "utm_source", "utm_campaign");
		InputStream in = new ByteArrayInputStream(url.getBytes());
		loader.bindTo(null, new BufferedPositionedInputStream(in), 0, url.length());
		Tuple tuple = loader.getNext();
		assertEquals("feed", (String) tuple.get(0));
		assertEquals("feedburner", (String) tuple.get(1));
		assertEquals("Feed: kotaku/full (Kotaku)", (String) tuple.get(2));
		assertEquals(3, tuple.size());
	}

	@Test
	public void testRepeatQueryParams() throws IOException {
		String url = "http://localhost/foo?a=123&a=456\nx=y\nhttp://localhost/bar?a=761&b=hi";
		QuerystringLoader loader = new QuerystringLoader("a", "b");
		InputStream in = new ByteArrayInputStream(url.getBytes());
		loader.bindTo(null, new BufferedPositionedInputStream(in), 0, url.length());
		Tuple tuple = loader.getNext();
		assertEquals("123", (String) tuple.get(0));
		assertNull(tuple.get(1));
		tuple = loader.getNext();
		assertEquals(2, tuple.size());
		assertNull(tuple.get(0));
		assertNull(tuple.get(1));
		tuple = loader.getNext();
		assertEquals("761", (String) tuple.get(0));
		assertEquals("hi", (String) tuple.get(1));
	}
}
