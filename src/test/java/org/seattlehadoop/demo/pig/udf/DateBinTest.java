package org.seattlehadoop.demo.pig.udf;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.Test;

public class DateBinTest {

	private DateBin m_dateBin;
	private Date m_baseDate;
	private final long m_baseDateBin = 2114109L;

	@Before
	public void setup() throws InstantiationException, IllegalAccessException {
		m_dateBin = DateBin.class.newInstance();
		m_baseDate = createDate(2010, 02, 12, 11, 30, 5);
	}

	private Tuple makeTuple(Date date, int delta) {
		DefaultTuple ret = new DefaultTuple();
		ret.append(DateBin.df.format(date));
		ret.append((Integer) delta);
		return ret;
	}

	private Tuple makeTuple(Date date, int delta, int addMillis) {
		DefaultTuple ret = new DefaultTuple();
		ret.append(DateBin.df.format(date));
		ret.append((Integer) delta);
		ret.append((Integer) addMillis);
		return ret;
	}

	public static Date createDate(int year, int month, int day, int hour, int minute, int second) {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month);
		cal.set(Calendar.DAY_OF_MONTH, day);
		cal.set(Calendar.HOUR, hour);
		cal.set(Calendar.MINUTE, minute);
		cal.set(Calendar.SECOND, second);
		return cal.getTime();
	}

	@Test
	public void testTwoEntries() throws IOException {
		long dateBin1 = m_dateBin.exec(makeTuple(m_baseDate, 60 * 10 * 1000));
		assertEquals(m_baseDateBin, dateBin1);
		Date date2 = new Date(m_baseDate.getTime() + 60 * 10 * 1000);
		long dateBin2 = m_dateBin.exec(makeTuple(date2, 60 * 10 * 1000));
		assertEquals(dateBin1 + 1, dateBin2);
	}

	@Test
	public void testThreeEntries() throws IOException {
		long dateBin1 = m_dateBin.exec(makeTuple(m_baseDate, 60 * 10 * 1000, 60 * 20 * 1000));
		assertEquals(m_baseDateBin + 2, dateBin1);
	}
}
