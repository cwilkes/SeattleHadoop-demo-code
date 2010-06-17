package org.seattlehadoop.demo.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.PigLineRecordReader;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class RegexLoader extends Utf8StorageConverter implements LoadFunc {

	public static final int ERROR_CODE = 8675309;
	private final Pattern m_linePattern;
	private PigLineRecordReader in;
	private long end;

	public RegexLoader(String pattern)  {
		m_linePattern = Pattern.compile(pattern);
	}

	@Override
	public void bindTo(String p_fileName, BufferedPositionedInputStream in, long offset, long end) throws IOException {
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
		Matcher m = m_linePattern.matcher(value.toString());
		if (!m.matches()) {
			return EmptyTuple.getInstance();
		}
		List<String> regexMatches = new ArrayList<String>();
		for (int i = 1; i <= m.groupCount(); i++) {
			regexMatches.add(m.group(i));
		}
		return mTupleFactory.newTupleNoCopy(regexMatches);
	}

}
