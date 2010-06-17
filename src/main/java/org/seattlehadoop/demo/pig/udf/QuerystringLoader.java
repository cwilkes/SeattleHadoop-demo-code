package org.seattlehadoop.demo.pig.udf;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.PigLineRecordReader;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.mortbay.jetty.HttpURI;
import org.mortbay.util.MultiMap;

public class QuerystringLoader extends Utf8StorageConverter implements LoadFunc {
	private PigLineRecordReader in;
	private long end;
	private final String[] m_fieldsInOrder;

	public QuerystringLoader(String... fieldsInOrder) {
		m_fieldsInOrder = fieldsInOrder;
	}

	@Override
	public void bindTo(String p_fileName, BufferedPositionedInputStream in, long offset, long end) throws IOException {
		String resourceStr = BufferedPositionedInputStream.class.getPackage().getName().replaceAll("\\.", "/");
		//resourceStr += "/PigLineRecordReader.class";
		resourceStr = "/" + resourceStr;
		URL resource = getClass().getResource(resourceStr);
		System.out.println("In is a " + in.getClass() + " at " + resource + "(" + resourceStr + ")");
		this.in = new PigLineRecordReader(in, offset, end);
		this.end = end;

		// Since we are not block aligned we throw away the first
		// record and count on a different instance to read it
		if (offset != 0) {
			getNext();
		}
	}

	@Override
	public Schema determineSchema(String p_fileName, ExecType p_execType, DataStorage p_storage) throws IOException {
		return null;
	}

	@Override
	public void fieldsToRead(Schema p_schema) {

	}

	@SuppressWarnings("unchecked")
	protected Map<String, Object> getParameterMap(String line) {
		HttpURI uri = new HttpURI(line);
		MultiMap map = new MultiMap();
		uri.decodeQueryTo(map);
		return map;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Tuple getNext() throws IOException {
		if (in == null || in.getPosition() > end) {
			return null;
		}
		Text value = new Text();
		boolean notDone = in.next(value);
		if (!notDone) {
			return null;
		}
		Map<String, Object> parameters = getParameterMap(value.toString());
		List<String> output = new ArrayList<String>();
		for (String fieldName : m_fieldsInOrder) {
			Object object = parameters.get(fieldName);
			if (object == null) {
				output.add(null);
				continue;
			}
			if (object instanceof String) {
				output.add((String) object);
			} else {
				List<String> objectVal = (List<String>) object;
				output.add(objectVal.get(0));
			}
		}
		return mTupleFactory.newTupleNoCopy(output);
	}

}