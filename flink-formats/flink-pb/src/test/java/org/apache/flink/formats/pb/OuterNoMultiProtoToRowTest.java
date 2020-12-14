package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.deserialize.PbRowDeserializationSchema;
import org.apache.flink.formats.pb.testproto.SimpleTestOuterNomultiProto;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

public class OuterNoMultiProtoToRowTest {
	@Test(expected = IllegalArgumentException.class)
	public void testSimple() {
		RowType rowType = PbRowTypeInformation.generateRowType(SimpleTestOuterNomultiProto.SimpleTestOuterNomulti
			.getDescriptor());
		new PbRowDeserializationSchema(
			rowType,
			InternalTypeInfo.of(rowType),
			SimpleTestOuterNomultiProto.SimpleTestOuterNomulti.class.getName(),
			false,
			false);
	}
}
