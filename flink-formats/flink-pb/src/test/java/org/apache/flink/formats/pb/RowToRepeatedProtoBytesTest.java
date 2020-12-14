/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.pb;

import org.apache.flink.formats.pb.serialize.PbRowSerializationSchema;
import org.apache.flink.formats.pb.testproto.RepeatedTest;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RowToRepeatedProtoBytesTest {
	@Test
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

	@Test
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

	@Test
	public void testNull() throws Exception {
		RowData row = GenericRowData.of(1, null, false, 0.1f, 0.01, StringData.fromString("hello"));
		byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, RepeatedTest.class);
		RepeatedTest repeatedTest = RepeatedTest.parseFrom(bytes);
		assertEquals(0, repeatedTest.getBCount());
	}
}
