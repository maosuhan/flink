package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.testproto.MapTest;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;

public class RowToMapProtoBytesTest extends TestCase {
	public void testSimple() throws Exception {
		Map map1 = new HashMap();
		map1.put(StringData.fromString("a"), StringData.fromString("b"));
		Map map2 = new HashMap();
		map2.put(StringData.fromString("c"), GenericRowData.of(1, 2L));
		RowData row = GenericRowData.of(1, new GenericMapData(map1), new GenericMapData(map2));

		byte[] bytes = FlinkProtobufHelper.rowToPbBytes(row, MapTest.class);

		MapTest mapTest = MapTest.parseFrom(bytes);
		assertEquals(1, mapTest.getA());
		assertEquals("b", mapTest.getMap1Map().get("a"));
		MapTest.InnerMessageTest innerMessageTest = mapTest.getMap2Map().get("c");
		assertEquals(1, innerMessageTest.getA());
		assertEquals(2L, innerMessageTest.getB());
	}

	public void testNull() throws Exception {
		RowData row = GenericRowData.of(1, null, null);

		byte[] bytes = FlinkProtobufHelper.rowToPbBytes(row, MapTest.class);
		MapTest mapTest = MapTest.parseFrom(bytes);
		Map<String, String> map = mapTest.getMap1Map();
		assertEquals(0, map.size());
	}
}
