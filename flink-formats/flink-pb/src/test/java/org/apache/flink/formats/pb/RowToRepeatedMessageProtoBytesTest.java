package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.serialize.PbRowSerializationSchema;
import org.apache.flink.formats.pb.testproto.RepeatedMessageTest;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RowToRepeatedMessageProtoBytesTest{
	@Test
	public void testRepeatedMessage() throws Exception {
		RowData subRow = GenericRowData.of(1, 2L);
		RowData subRow2 = GenericRowData.of(3, 4L);
		ArrayData tmp = new GenericArrayData(new Object[]{subRow, subRow2});
		RowData row = GenericRowData.of(tmp);

		RowType rowType = PbRowTypeInformation.generateRowType(RepeatedMessageTest.getDescriptor());
		row = ProtobufTestHelper.validateRow(row, rowType);

		PbRowSerializationSchema serializationSchema = new PbRowSerializationSchema(
			rowType,
			RepeatedMessageTest.class.getName());

		byte[] bytes = serializationSchema.serialize(row);
		RepeatedMessageTest repeatedMessageTest = RepeatedMessageTest.parseFrom(bytes);

		assertEquals(2, repeatedMessageTest.getDCount());

		assertEquals(1, repeatedMessageTest.getD(0).getA());
		assertEquals(2L, repeatedMessageTest.getD(0).getB());
		assertEquals(3, repeatedMessageTest.getD(1).getA());
		assertEquals(4L, repeatedMessageTest.getD(1).getB());
	}
}
