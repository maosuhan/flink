package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.deserialize.PbRowDeserializationSchema;
import org.apache.flink.formats.pb.testproto.OneofTest;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OneofProtoToRowTest{
	@Test
	public void testSimple() throws Exception {
		RowType rowType = PbRowTypeInformation.generateRowType(OneofTest.getDescriptor());
		PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(
			rowType,
			InternalTypeInfo.of(rowType),
			OneofTest.class.getName(),
			false,
			false);

		OneofTest oneofTest = OneofTest.newBuilder()
			.setA(1)
			.setB(2)
			.build();

		RowData row = deserializationSchema.deserialize(oneofTest.toByteArray());
		row = ProtobufTestHelper.validateRow(row, rowType);

		assertTrue(row.isNullAt(0));
		assertEquals(2, row.getInt(1));
	}

}
