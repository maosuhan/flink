package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.deserialize.PbRowDeserializationSchema;
import org.apache.flink.formats.pb.testproto.MapTest;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MapProtoToRowTest{
	@Test
	public void testMessage() throws Exception {
		RowType rowType = PbRowTypeInformation.generateRowType(MapTest.getDescriptor());
		PbRowDeserializationSchema deserializationSchema = new PbRowDeserializationSchema(
			rowType,
			InternalTypeInfo.of(rowType),
			MapTest.class.getName(), false, true);

		MapTest.InnerMessageTest innerMessageTest = MapTest.InnerMessageTest
			.newBuilder()
			.setA(1)
			.setB(2)
			.build();
		MapTest mapTest = MapTest.newBuilder()
			.setA(1)
			.putMap1("a", "b")
			.putMap1("c", "d")
			.putMap2("f", innerMessageTest).build();

		RowData row = deserializationSchema.deserialize(mapTest.toByteArray());
		row = ProtobufTestHelper.validateRow(row, rowType);

		MapData map1 = row.getMap(1);
		assertEquals("a", map1.keyArray().getString(0).toString());
		assertEquals("b", map1.valueArray().getString(0).toString());
		assertEquals("c", map1.keyArray().getString(1).toString());
		assertEquals("d", map1.valueArray().getString(1).toString());

		MapData map2 = row.getMap(2);
		assertEquals("f", map2.keyArray().getString(0).toString());
		RowData rowData2 = map2.valueArray().getRow(0, 2);

		assertEquals(1, rowData2.getInt(0));
		assertEquals(2L, rowData2.getLong(1));
	}
}
