package org.seattlehadoop.pig.demo.storevisits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.seattlehadoop.demo.pig.udf.DateBinUtils;

public class DateBin2 extends EvalFunc<Tuple> {

	@Override
	public Tuple exec(Tuple input) throws IOException {
		long bin = DateBinUtils.findBin(input, this);
		if (bin == -1) {
			return null;
		}
		Tuple ret = TupleFactory.getInstance().newTuple(2);
		ret.set(0, bin);
		ret.set(1, bin + 1);
		return ret;
	}

	@Override
	public Schema outputSchema(Schema p_input) {
		return new Schema(Arrays.asList(new Schema.FieldSchema("bin1", DataType.LONG), new Schema.FieldSchema("bin2", DataType.LONG)));
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
