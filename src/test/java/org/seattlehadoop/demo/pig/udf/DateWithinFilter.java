package org.seattlehadoop.demo.pig.udf;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.pig.FilterFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class DateWithinFilter extends FilterFunc {
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private Date[] getColumnDates(Tuple input) throws ExecException {
		String strDate1 = (String) input.get(0);
		String strDate2 = (String) input.get(1);
		if (strDate1 == null || strDate2 == null) {
			return null;
		}
		Date date1 = null;
		try {
			date1 = df.parse(strDate1);
		} catch (ParseException e) {
			String msg = "DateBin : Parameters have to be string in '" + df.toString() + "' format, not '" + strDate1 + "'";
			warn(msg, PigWarning.UDF_WARNING_1);
			return null;
		}
		Date date2 = null;
		try {
			date2 = df.parse(strDate2);
		} catch (ParseException e) {
			String msg = "DateBin : Parameters have to be string in '" + df.toString() + "' format, not '" + strDate2 + "'";
			warn(msg, PigWarning.UDF_WARNING_1);
			return null;
		}
		return new Date[] { date1, date2 };
	}

	@Override
	public Boolean exec(Tuple input) throws IOException {
		if (input.size() != 3) {
			throw new IOException("DateBin : Need at least two parameters, the column and the bin size in milliseconds");
		}
		Date[] startAndTryDates = getColumnDates(input);
		if (startAndTryDates == null)
			return false;
		long dateDiff = startAndTryDates[1].getTime() - startAndTryDates[0].getTime();
		if (dateDiff < 0) {
			return false;
		}
		int maxDateDiff = (Integer) input.get(2);
		return dateDiff <= maxDateDiff;
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		Schema s = new Schema();
		s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		s.add(new Schema.FieldSchema(null, DataType.INTEGER));
		funcList.add(new FuncSpec(this.getClass().getName(), s));
		return funcList;
	}
}
