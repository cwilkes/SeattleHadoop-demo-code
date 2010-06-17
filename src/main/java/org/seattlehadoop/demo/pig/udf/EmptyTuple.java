package org.seattlehadoop.demo.pig.udf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class EmptyTuple implements Tuple {

	private static final long serialVersionUID = 2238196736512773270L;
	private static final EmptyTuple INSTANCE;
	static {
		INSTANCE = new EmptyTuple();
	}

	public static EmptyTuple getInstance() {
		return INSTANCE;
	}

	@Override
	public void append(Object p_val) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object get(int p_fieldNum) throws ExecException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Object> getAll() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getMemorySize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte getType(int p_fieldNum) throws ExecException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isNull() {
		return true;
	}

	@Override
	public boolean isNull(int p_fieldNum) throws ExecException {
		return true;
	}

	@Override
	public void reference(Tuple p_t) {
		// TODO Auto-generated method stub

	}

	@Override
	public void set(int p_fieldNum, Object p_val) throws ExecException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNull(boolean p_isNull) {
		// TODO Auto-generated method stub

	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public String toDelimitedString(String p_delim) throws ExecException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void readFields(DataInput p_in) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(DataOutput p_out) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public int compareTo(Object p_o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
