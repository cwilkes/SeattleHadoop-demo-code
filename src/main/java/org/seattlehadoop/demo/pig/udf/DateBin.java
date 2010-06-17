package org.seattlehadoop.demo.pig.udf;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class DateBin extends EvalFunc<Long> {

	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	public Long exec(Tuple input) throws IOException {
		return DateBinUtils.findBin(input, this);
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
