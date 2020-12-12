package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.deserialize.PbRowDeserializationSchema;
import org.apache.flink.formats.pb.testproto.OneofTest;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import junit.framework.TestCase;

public class OneofProtoToRowTest extends TestCase {
	public void testSimple() throws Exception {
		RowType rowType = PbRowTypeInformation.generateRowType(OneofTest.getDescriptor());
		PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(
			rowType,
			InternalTypeInfo.of(rowType),
			OneofTest.class.getName(),
			false,
			true);

		OneofTest oneofTest = OneofTest.newBuilder()
			.setA(1)
			.setB(2)
			.build();

		RowData row = deserializationSchema.deserialize(oneofTest.toByteArray());
		row = FlinkProtobufHelper.validateRow(row, rowType);

		assertTrue(row.isNullAt(0));
		assertEquals(2, row.getInt(1));
	}

}
