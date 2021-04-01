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

package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.testproto.OneofTest;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test conversion of flink internal map data to one_of proto data. */
public class OneofRowToProtoTest {
    @Test
    public void testSimpleWithInvalidInput() throws Exception {
        // testing with an invalid inputs: a user should not specify several values of a oneof
        RowData row = GenericRowData.of(1, 2, GenericRowData.of(3, 4), GenericRowData.of(5, 6));

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, OneofTest.class);
        OneofTest oneofTest = OneofTest.parseFrom(bytes);

        assertFalse(oneofTest.hasA());
        assertTrue(oneofTest.hasB());
        assertEquals(2, oneofTest.getB());

        assertFalse(oneofTest.hasC());
        assertTrue(oneofTest.hasD());
        assertEquals(OneofTest.InnerB.newBuilder().setBc(5).setBd(6).build(), oneofTest.getD());
    }

    @Test
    public void testSimpleWithvalidInputFirstValues() throws Exception {
        // testing with an valid input: only the 1rst value of the oneof is specified
        RowData row = GenericRowData.of(1, null, GenericRowData.of(3, 4), null);

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, OneofTest.class);
        OneofTest oneofTest = OneofTest.parseFrom(bytes);

        assertTrue(oneofTest.hasA());
        assertEquals(1, oneofTest.getA());
        assertFalse(oneofTest.hasB());

        assertTrue(oneofTest.hasC());
        assertEquals(OneofTest.InnerA.newBuilder().setAc(3).setAd(4).build(), oneofTest.getC());
        assertFalse(oneofTest.hasD());
    }

    @Test
    public void testSimpleWithvalidInputSecondValues() throws Exception {
        // testing with an valid input: only the 2nd value of the oneof is specified
        RowData row = GenericRowData.of(null, 2, null, GenericRowData.of(5, 6));

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, OneofTest.class);
        OneofTest oneofTest = OneofTest.parseFrom(bytes);

        assertFalse(oneofTest.hasA());
        assertTrue(oneofTest.hasB());
        assertEquals(2, oneofTest.getB());

        assertFalse(oneofTest.hasC());
        assertTrue(oneofTest.hasD());
        assertEquals(OneofTest.InnerB.newBuilder().setBc(5).setBd(6).build(), oneofTest.getD());
    }
}
