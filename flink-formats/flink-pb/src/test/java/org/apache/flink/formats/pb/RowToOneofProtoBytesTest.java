package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.testproto.OneofTest;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RowToOneofProtoBytesTest {
	@Test
	public void testSimple() throws Exception {
		RowData row = GenericRowData.of(1, 2);

		byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, OneofTest.class);
		OneofTest oneofTest = OneofTest.parseFrom(bytes);
		assertFalse(oneofTest.hasA());
		assertEquals(2, oneofTest.getB());
	}

}
