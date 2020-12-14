package org.apache.flink.formats.pb.deserialize;

import com.google.protobuf.Descriptors;

public class PbCodegenSimpleDeserializer implements PbCodegenDeserializer {
	private Descriptors.FieldDescriptor fd;

	public PbCodegenSimpleDeserializer(
		Descriptors.FieldDescriptor fd) {
		this.fd = fd;
	}

	@Override
	public String codegen(String returnVarName, String messageGetStr) {
		StringBuilder sb = new StringBuilder();
		switch (fd.getJavaType()) {
			case INT:
			case LONG:
			case FLOAT:
			case DOUBLE:
			case BOOLEAN:
				sb.append(returnVarName + " = " + messageGetStr + ";");
				break;
			case BYTE_STRING:
				sb.append(returnVarName + " = " + messageGetStr + ".toByteArray();");
				break;
			case STRING:
			case ENUM:
				sb.append(
					returnVarName + " = StringData.fromString(" + messageGetStr + ".toString());");
				break;
		}
		return sb.toString();
	}
}
