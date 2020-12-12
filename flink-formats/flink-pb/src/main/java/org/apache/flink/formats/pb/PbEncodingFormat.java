package org.apache.flink.formats.pb;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.pb.serialize.PbRowSerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class PbEncodingFormat implements EncodingFormat<SerializationSchema<RowData>> {
	private String messageClassName;

	public PbEncodingFormat(String messageClassName) {
		this.messageClassName = messageClassName;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public SerializationSchema<RowData> createRuntimeEncoder(
		DynamicTableSink.Context context,
		DataType consumedDataType) {
		RowType rowType = (RowType) consumedDataType.getLogicalType();
		return new PbRowSerializationSchema(rowType, this.messageClassName);
	}
}
