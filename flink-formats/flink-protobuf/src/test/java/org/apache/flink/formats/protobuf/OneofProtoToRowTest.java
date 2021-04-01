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

import org.apache.flink.formats.protobuf.deserialize.PbRowDataDeserializationSchema;
import org.apache.flink.formats.protobuf.testproto.OneofTest;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test conversion of proto one_of data to flink internal data. */
public class OneofProtoToRowTest {

    @Test
    public void testSimple() throws Exception {
        RowType rowType = PbRowTypeInformationUtil.generateRowType(OneofTest.getDescriptor());
        PbRowDataDeserializationSchema deserializationSchema =
                new PbRowDataDeserializationSchema(
                        rowType,
                        InternalTypeInfo.of(rowType),
                        OneofTest.class.getName(),
                        false,
                        false);

        OneofTest oneofTest =
                OneofTest.newBuilder()
                        // the provider of this protobuf is incorrectly setting both A and B,
                        // which are a oneof of scalar types
                        .setA(1)
                        .setB(2)
                        // the provider of this protobuf is incorrectly setting both A and B,
                        // which are a oneof of nested types
                        .setC(OneofTest.InnerA.newBuilder().setAc(3).setAd(4).build())
                        .setD(OneofTest.InnerB.newBuilder().setBc(5).setBd(6).build())
                        .build();

        RowData row = deserializationSchema.deserialize(oneofTest.toByteArray());
        row = ProtobufTestHelper.validateRow(row, rowType);

        // this fails, although that seems to me a limitation of protobuf itself, which should
        // now allow a user to specify both values of the oneof, especially in case of primitives
        //        assertTrue(row.isNullAt(0));
        assertEquals(2, row.getInt(1));

        // C should be null and be overridden by D
        assertTrue(row.isNullAt(2));
        // D should have the expected value for bc and bd
        assertEquals(5, row.getRow(3, 2).getInt(0));
        assertEquals(6, row.getRow(3, 2).getInt(1));
    }
}
