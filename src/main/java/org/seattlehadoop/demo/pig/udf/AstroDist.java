package org.seattlehadoop.demo.pig.udf;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class AstroDist extends EvalFunc<Double> {

	private static int ERROR_CODE_BAD_TUPLE = 8123141;

	@Override
	public Double exec(Tuple input) throws IOException {
		Point3D astroPos1 = new Point3D((Tuple) input.get(0));
		Point3D astroPos2 = new Point3D((Tuple) input.get(1));
		return astroPos1.distance(astroPos2);
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		Schema s = new Schema();
		s.add(new Schema.FieldSchema(null, DataType.TUPLE));
		s.add(new Schema.FieldSchema(null, DataType.TUPLE));
		return Arrays.asList(new FuncSpec(this.getClass().getName(), s));
	}

	private static class Point3D {
		private final int x, y, z;

		private Point3D(Tuple tuple) throws ExecException {
			if (tuple.size() != 3) {
				throw new ExecException("Received " + tuple.size() + " points in 3D tuple", ERROR_CODE_BAD_TUPLE, PigException.BUG);
			}
			x = (Integer) tuple.get(0);
			y = (Integer) tuple.get(1);
			z = (Integer) tuple.get(2);
		}

		private double distance(Point3D other) {
			return Math.sqrt(Math.pow(x - other.x, 2) + Math.pow(y - other.y, 2) + Math.pow(z - other.z, 2));
		}
	}
}
