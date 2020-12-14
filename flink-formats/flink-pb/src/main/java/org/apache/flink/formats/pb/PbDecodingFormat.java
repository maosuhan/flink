package org.apache.flink.formats.pb;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.pb.deserialize.PbRowDeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class PbDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {
	private String messageClassName;
	private boolean ignoreParseErrors;
	private boolean readDefaultValues;

	public PbDecodingFormat(
		String messageClassName,
		boolean ignoreParseErrors,
		boolean readDefaultValues) {
		this.messageClassName = messageClassName;
		this.ignoreParseErrors = ignoreParseErrors;
		this.readDefaultValues = readDefaultValues;
	}

	@Override
	public DeserializationSchema<RowData> createRuntimeDecoder(
		DynamicTableSource.Context context,
		DataType producedDataType) {
		final RowType rowType = (RowType) producedDataType.getLogicalType();
		final TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(
			producedDataType);
		return new PbRowDeserializationSchema(
			rowType,
			rowDataTypeInfo,
			this.messageClassName,
			this.ignoreParseErrors,
			this.readDefaultValues);
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}
}
