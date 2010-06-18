package org.seattlehadoop.demo.pig.udf;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class DateBin extends EvalFunc<Long> {

	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	public Long exec(Tuple input) throws IOException {
		if (input.size() < 2) {
			throw new IOException("DateBin : Need at least two parameters, the column and the bin size in milliseconds");
		}
		int pos = 0;
		String strDate = (String) input.get(pos++);
		if (strDate == null) {
			String msg = "DateBin : Parameters have to be string in '" + df.toString() + "' format, not '" + strDate + "'";
			warn(msg, PigWarning.UDF_WARNING_1);
			return null;
		}
		Date date1 = null;
		try {
			date1 = df.parse(strDate);
		} catch (ParseException e) {
			return null;
		}
		int binSize = (Integer) input.get(pos++);
		int deltaAdd = 0;
		if (pos < input.size())
			deltaAdd = (Integer) input.get(pos++);
		return (date1.getTime() + deltaAdd) / binSize;
	}

	@Override
	public Schema outputSchema(Schema p_input) {
		return new Schema(new Schema.FieldSchema(null, DataType.LONG));
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		Schema s = new Schema();
		s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		s.add(new Schema.FieldSchema(null, DataType.INTEGER));
		funcList.add(new FuncSpec(this.getClass().getName(), s));
		return funcList;
	}

}
