package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.deserialize.PbRowDeserializationSchema;
import org.apache.flink.formats.pb.testproto.RepeatedTest;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import junit.framework.TestCase;

public class RepeatedProtoToRowTest extends TestCase {
	public void testRepeated() throws Exception {
		RowType rowType = PbRowTypeInformation.generateRowType(RepeatedTest.getDescriptor());
		PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(
			rowType,
			InternalTypeInfo.of(rowType),
			RepeatedTest.class.getName(),
			false,
			true);

		RepeatedTest simple = RepeatedTest.newBuilder()
			.setA(1)
			.addB(1)
			.addB(2)
			.build();

		RowData row = deserializationSchema.deserialize(simple.toByteArray());
		row = ProtobufTestHelper.validateRow(row, rowType);

		assertEquals(6, row.getArity());
		assertEquals(1, row.getInt(0));
		ArrayData arr = row.getArray(1);
		assertEquals(2, arr.size());
		assertEquals(1L, arr.getLong(0));
		assertEquals(2L, arr.getLong(1));

	}
}
