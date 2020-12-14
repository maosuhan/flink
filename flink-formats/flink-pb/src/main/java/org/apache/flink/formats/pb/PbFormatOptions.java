package org.apache.flink.formats.pb;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class PbFormatOptions {
	public static final ConfigOption<String> MESSAGE_CLASS_NAME = ConfigOptions
		.key("message-class-name")
		.stringType()
		.noDefaultValue()
		.withDescription(
			"Required option to specify the full name of protobuf message class. The protobuf class "
				+ "must be located in the classpath both in client and task side");

	public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions
		.key("ignore-parse-errors")
		.booleanType()
		.defaultValue(false)
		.withDescription(
			"Optional flag to skip rows with parse errors instead of failing; false by default.");

	public static final ConfigOption<Boolean> READ_DEFAULT_VALUES = ConfigOptions
		.key("read-default-values")
		.booleanType()
		.defaultValue(false)
		.withDescription(
			"Optional flag to read as default values instead of null when some field does not exist in deserialization; default to false."
				+ "If proto syntax is proto3, this value will be set true forcibly because proto3's standard is to use default values.");
}
