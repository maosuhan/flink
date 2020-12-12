package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.deserialize.PbRowDeserializationSchema;
import org.apache.flink.formats.pb.testproto.MultipleLevelMessageTest;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import junit.framework.TestCase;

public class MultiLevelMessageProtoToRowTest extends TestCase {
	public void testMessage() throws Exception {
		RowType rowType = PbRowTypeInformation.generateRowType(MultipleLevelMessageTest.getDescriptor());
		PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(
			rowType,
			InternalTypeInfo.of(rowType),
			MultipleLevelMessageTest.class.getName(),
			false,
			true);

		MultipleLevelMessageTest.InnerMessageTest1.InnerMessageTest2 innerMessageTest2 = MultipleLevelMessageTest.InnerMessageTest1.InnerMessageTest2
			.newBuilder()
			.setA(1)
			.setB(2L)
			.build();
		MultipleLevelMessageTest.InnerMessageTest1 innerMessageTest = MultipleLevelMessageTest.InnerMessageTest1
			.newBuilder()
			.setC(false)
			.setA(innerMessageTest2)
			.build();
		MultipleLevelMessageTest multipleLevelMessageTest = MultipleLevelMessageTest.newBuilder()
			.setD(innerMessageTest)
			.setA(1)
			.build();

		RowData row = deserializationSchema.deserialize(multipleLevelMessageTest.toByteArray());
		row = ProtobufTestHelper.validateRow(row, rowType);

		assertEquals(4, row.getArity());
		RowData subRow = (RowData) row.getRow(3, 2);
		assertFalse(subRow.getBoolean(1));

		RowData subSubRow = (RowData) subRow.getRow(0, 2);
		assertEquals(1, subSubRow.getInt(0));
		assertEquals(2L, subSubRow.getLong(1));
	}
}
