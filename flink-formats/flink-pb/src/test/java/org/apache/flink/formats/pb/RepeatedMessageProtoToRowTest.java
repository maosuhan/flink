package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.deserialize.PbRowDeserializationSchema;
import org.apache.flink.formats.pb.testproto.RepeatedMessageTest;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RepeatedMessageProtoToRowTest{
	@Test
	public void testRepeatedMessage() throws Exception {
		RowType rowType = PbRowTypeInformation.generateRowType(RepeatedMessageTest.getDescriptor());
		PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(
			rowType,
			InternalTypeInfo.of(rowType),
			RepeatedMessageTest.class.getName(),
			false,
			true);

		RepeatedMessageTest.InnerMessageTest innerMessageTest = RepeatedMessageTest.InnerMessageTest
			.newBuilder()
			.setA(1)
			.setB(2L)
			.build();

		RepeatedMessageTest.InnerMessageTest innerMessageTest1 = RepeatedMessageTest.InnerMessageTest
			.newBuilder()
			.setA(3)
			.setB(4L)
			.build();

		RepeatedMessageTest repeatedMessageTest = RepeatedMessageTest.newBuilder()
			.addD(innerMessageTest)
			.addD(innerMessageTest1)
			.build();

		RowData row = deserializationSchema.deserialize(repeatedMessageTest.toByteArray());
		row = ProtobufTestHelper.validateRow(row, rowType);

		ArrayData objs = row.getArray(0);
		RowData subRow = objs.getRow(0, 2);
		assertEquals(1, subRow.getInt(0));
		assertEquals(2L, subRow.getLong(1));
		subRow = objs.getRow(1, 2);
		assertEquals(3, subRow.getInt(0));
		assertEquals(4L, subRow.getLong(1));
	}
}
