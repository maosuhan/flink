package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.deserialize.PbRowDeserializationSchema;
import org.apache.flink.formats.pb.testproto.SimpleTestOuterMulti;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.ByteString;
import junit.framework.TestCase;

public class OuterMultiProtoToRowTest extends TestCase {
	public void testSimple() throws Exception {
		RowType rowType = PbRowTypeInformation.generateRowType(SimpleTestOuterMulti.getDescriptor());
		PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(
			rowType,
			InternalTypeInfo.of(rowType),
			SimpleTestOuterMulti.class.getName(),
			false,
			true);

		SimpleTestOuterMulti simple = SimpleTestOuterMulti.newBuilder()
			.setA(1)
			.setB(2L)
			.setC(false)
			.setD(0.1f)
			.setE(0.01)
			.setF("haha")
			.setG(ByteString.copyFrom(new byte[]{1}))
			.build();

		RowData row = deserializationSchema.deserialize(simple.toByteArray());
		row = ProtobufTestHelper.validateRow(row, rowType);

		assertEquals(7, row.getArity());
		assertEquals(1, row.getInt(0));
		assertEquals(2L, row.getLong(1));
		assertFalse(row.getBoolean(2));
		assertEquals(0.1f, row.getFloat(3));
		assertEquals(0.01, row.getDouble(4));
		assertEquals("haha", row.getString(5).toString());
		assertEquals(1, (row.getBinary(6))[0]);
	}
}
