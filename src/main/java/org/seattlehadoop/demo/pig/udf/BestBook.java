package org.seattlehadoop.demo.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class BestBook extends EvalFunc<Tuple> implements Algebraic {
	private static TupleFactory m_tupleFactory = TupleFactory.getInstance();
	private static BagFactory m_bagFactory = BagFactory.getInstance();

	@Override
	public Tuple exec(Tuple p_input) throws IOException {
		Iterator<Tuple> bagReviewers = ((DataBag) p_input.get(0)).iterator();
		Iterator<Tuple> bagScores = ((DataBag) p_input.get(1)).iterator();
		int bestScore = -1;
		String bestReviewer = null;
		while (bagReviewers.hasNext()) {
			String reviewerName = (String) bagReviewers.next().get(0);
			Integer score = (Integer) bagScores.next().get(0);
			if (score.intValue() > bestScore) {
				bestScore = score;
				bestReviewer = reviewerName;
			}
		}
		Tuple ret = m_tupleFactory.newTuple(2);
		ret.set(0, bestReviewer);
		ret.set(1, bestScore);
		return ret;
	}

	@Override
	public Schema outputSchema(Schema p_input) {
		try {
			return Schema.generateNestedSchema(DataType.TUPLE, DataType.CHARARRAY, DataType.INTEGER);
		} catch (FrontendException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		// this is annoying
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.BAG))));
		// Schema s = Schema.generateNestedSchema(DataType.BAG,
		// DataType.CHARARRAY, DataType.INTEGER);
		Schema s = new Schema(new Schema.FieldSchema(null, DataType.BAG));
		funcList.add(new FuncSpec(this.getClass().getName(), s));
		return null;
	}

	protected static Tuple makeSingleTupleBookReturn2(Tuple p_input) throws ExecException {

		int bestScore = -1;
		String bestReviewer = null;
		for (Iterator<Tuple> it = ((DataBag) p_input.get(0)).iterator(); it.hasNext();) {
			Tuple t = it.next();
			String reviewerName = (String) t.get(0);
			Integer score = (Integer) t.get(1);
			System.out.println(" looking at " + reviewerName + " " + score);
			if (score.intValue() > bestScore) {
				bestScore = score;
				bestReviewer = reviewerName;
			}
		}
		Tuple ret = m_tupleFactory.newTuple(2);
		ret.set(0, bestReviewer);
		ret.set(1, bestScore);
		DataBag bag = m_bagFactory.newDefaultBag();
		bag.add(ret);
		return m_tupleFactory.newTuple(bag);
	}

	public static class Initial extends EvalFunc<Tuple> {

		@Override
		public Tuple exec(Tuple p_input) throws IOException {
			Iterator<Tuple> bagReviewers = ((DataBag) p_input.get(0)).iterator();
			Iterator<Tuple> bagScores = ((DataBag) p_input.get(1)).iterator();
			if (bagReviewers.hasNext()) {
				String reviewerName = (String) bagReviewers.next().get(0);
				Integer score = (Integer) bagScores.next().get(0);
				Tuple ret = m_tupleFactory.newTuple(2);
				ret.set(0, reviewerName);
				ret.set(1, score);
				return ret;
			} else {
				return null;
			}
		}
	}

	public static class Intermediate extends EvalFunc<Tuple> {
		@Override
		public Tuple exec(Tuple p_input) throws IOException {
			return makeSingleTupleBookReturn2(p_input);
		}

	}

	public static class Final extends EvalFunc<Tuple> {
		@Override
		public Tuple exec(Tuple p_input) throws IOException {
			Iterator<Tuple> it = ((DataBag) p_input.get(0)).iterator();
			if (it.hasNext()) {
				return m_tupleFactory.newTuple(it.next());
			} else {
				return null;
			}
		}
	}

	public String getInitial() {
		return Initial.class.getName();
	}

	public String getIntermed() {
		return Intermediate.class.getName();
	}

	public String getFinal() {
		return Final.class.getName();
	}
}
