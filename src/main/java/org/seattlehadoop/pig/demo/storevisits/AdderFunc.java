package org.seattlehadoop.pig.demo.storevisits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class AdderFunc extends EvalFunc<Long> {

	@Override
	public Long exec(Tuple p_input) throws IOException {
		long base = (Long) p_input.get(0);
		int add = 1;
		if (p_input.size() > 1) {
			add = (Integer) p_input.get(1);
		}
		return base + add;
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		Schema s = new Schema();
		s.add(new Schema.FieldSchema(null, DataType.LONG));
		s.add(new Schema.FieldSchema(null, DataType.INTEGER));
		funcList.add(new FuncSpec(this.getClass().getName(), s));
		return funcList;
	}
}
