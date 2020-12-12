package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.serialize.PbRowSerializationSchema;
import org.apache.flink.formats.pb.testproto.RepeatedTest;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import junit.framework.TestCase;

public class RowToRepeatedProtoBytesTest extends TestCase {
	public void testSimple() throws Exception {
		RowData row = GenericRowData.of(
			1,
			new GenericArrayData(new Object[]{1L, 2L, 3L}),
			false,
			0.1f,
			0.01,
			StringData.fromString("hello"));

		RowType rowType = PbRowTypeInformation.generateRowType(RepeatedTest.getDescriptor());
		row = ProtobufTestHelper.validateRow(row, rowType);

		PbRowSerializationSchema serializationSchema = new PbRowSerializationSchema(
			rowType,
			RepeatedTest.class.getName());

		byte[] bytes = serializationSchema.serialize(row);
		RepeatedTest repeatedTest = RepeatedTest.parseFrom(bytes);
		assertEquals(3, repeatedTest.getBCount());
		assertEquals(1L, repeatedTest.getB(0));
		assertEquals(2L, repeatedTest.getB(1));
		assertEquals(3L, repeatedTest.getB(2));
	}

	public void testEmptyArray() throws Exception {
		RowData row = GenericRowData.of(
			1,
			new GenericArrayData(new Object[]{}),
			false,
			0.1f,
			0.01,
			StringData.fromString("hello"));

		RowType rowType = PbRowTypeInformation.generateRowType(RepeatedTest.getDescriptor());
		row = ProtobufTestHelper.validateRow(row, rowType);

		PbRowSerializationSchema serializationSchema = new PbRowSerializationSchema(
			rowType,
			RepeatedTest.class.getName());

		byte[] bytes = serializationSchema.serialize(row);
		RepeatedTest repeatedTest = RepeatedTest.parseFrom(bytes);
		assertEquals(0, repeatedTest.getBCount());
	}

	public void testNull() throws Exception {
		RowData row = GenericRowData.of(1, null, false, 0.1f, 0.01, StringData.fromString("hello"));
		byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, RepeatedTest.class);
		RepeatedTest repeatedTest = RepeatedTest.parseFrom(bytes);
		assertEquals(0, repeatedTest.getBCount());
	}
}
