package org.seattlehadoop.demo.pig.udf;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;

public class DateBinUtils {
	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static long findBin(Tuple input, EvalFunc<?> evalFunc) throws IOException {
		if (input.size() < 2) {
			throw new IOException("DateBin : Need at least two parameters, the column and the bin size in milliseconds");
		}
		int pos = 0;
		String strDate = (String) input.get(pos++);
		if (strDate == null) {
			String msg = "DateBin : Parameters have to be string in '" + df.toString() + "' format, not '" + strDate + "'";
			evalFunc.warn(msg, PigWarning.UDF_WARNING_1);
			return -1;
		}
		Date date1 = null;
		try {
			date1 = df.parse(strDate);
		} catch (ParseException e) {
			return -1;
		}
		int binSize = (Integer) input.get(pos++);
		int deltaAdd = 0;
		if (pos < input.size())
			deltaAdd = (Integer) input.get(pos++);
		return (date1.getTime() + deltaAdd) / binSize;
	}
}
