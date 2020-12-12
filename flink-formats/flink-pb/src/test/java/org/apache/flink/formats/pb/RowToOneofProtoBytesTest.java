package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.testproto.OneofTest;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import junit.framework.TestCase;

public class RowToOneofProtoBytesTest extends TestCase {
	public void testSimple() throws Exception {
		RowData row = GenericRowData.of(1, 2);

		byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, OneofTest.class);
		OneofTest oneofTest = OneofTest.parseFrom(bytes);
		assertFalse(oneofTest.hasA());
		assertEquals(2, oneofTest.getB());
	}

}
