package org.apache.flink.formats.pb;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.HashSet;
import java.util.Set;

public class PbFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

	public static final String IDENTIFIER = "pb";

	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
		DynamicTableFactory.Context context,
		ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);
		final String messageClassName = formatOptions.get(PbFormatOptions.MESSAGE_CLASS_NAME);
		boolean ignoreParseErrors = formatOptions.get(PbFormatOptions.IGNORE_PARSE_ERRORS);
		boolean readDefaultValues = formatOptions.get(PbFormatOptions.READ_DEFAULT_VALUES);
		return new PbDecodingFormat(messageClassName, ignoreParseErrors, readDefaultValues);
	}

	@Override
	public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
		DynamicTableFactory.Context context,
		ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);
		final String messageClassName = formatOptions.get(PbFormatOptions.MESSAGE_CLASS_NAME);
		return new PbEncodingFormat(messageClassName);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> result = new HashSet<>();
		result.add(PbFormatOptions.MESSAGE_CLASS_NAME);
		return result;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> result = new HashSet<>();
		result.add(PbFormatOptions.IGNORE_PARSE_ERRORS);
		result.add(PbFormatOptions.READ_DEFAULT_VALUES);
		return result;
	}

}
