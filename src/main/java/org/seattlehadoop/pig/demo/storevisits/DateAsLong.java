package org.seattlehadoop.pig.demo.storevisits;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;

public class DateAsLong extends EvalFunc<Long> {

	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	@Override
	public Long exec(Tuple input) throws IOException {
		String strDate = (String) input.get(0);
		if (strDate == null)
			return null;
		try {
			return df.parse(strDate).getTime();
		} catch (ParseException e) {
			String msg = "DateBin : Parameters have to be string in '" + df.toString() + "' format, not '" + strDate + "'";
			warn(msg, PigWarning.UDF_WARNING_1);
			return null;
		}
	}

}
