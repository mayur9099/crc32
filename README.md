1)package org.apache.hive.test.udf;

import java.util.Collection;
import java.util.Objects;
import java.util.zip.CRC32;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class SampleCRC32 extends UDF {
	public LongWritable evaluate(Text input) throws UDFArgumentException {
		if (input == null)
			return null;
		if (input instanceof Collection)
			throw new UDFArgumentException("This function takes only single text string.");
		CRC32 crc = new CRC32();
		crc.update(Objects.toString(input).getBytes());
		return new LongWritable(crc.getValue());
	}
}





2)package org.apache.hive.test.udf;

import static org.junit.Assert.*;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class SampleSRC32Test {

	@Test
	public void testEvaluate() throws UDFArgumentException {
		SampleCRC32 crc = new SampleCRC32();
		assertNull("Should return null on null argument", crc.evaluate(null));
		assertEquals(new LongWritable(3632233996l), crc.evaluate(new Text("test")));
		
	}

}
