package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.deserialize.PbRowDeserializationSchema;
import org.apache.flink.formats.pb.testproto.SimpleTestNoouterNomultiOuterClass;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

public class NoouterNomultiProtoToRowTest {
	@Test(expected = IllegalArgumentException.class)
	public void testSimple() {
		RowType rowType = PbRowTypeInformation.generateRowType(SimpleTestNoouterNomultiOuterClass.SimpleTestNoouterNomulti
			.getDescriptor());
		new PbRowDeserializationSchema(
			rowType,
			InternalTypeInfo.of(rowType),
			SimpleTestNoouterNomultiOuterClass.SimpleTestNoouterNomulti.class.getName(),
			false,
			true);
	}
}
