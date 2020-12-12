package org.apache.flink.formats.pb;

import com.google.protobuf.Descriptors;

import org.apache.flink.formats.pb.serialize.PbCodegenSerializeFactory;
import org.apache.flink.formats.pb.serialize.PbCodegenSerializer;
import org.apache.flink.table.types.logical.LogicalType;

public class PbCodegenUtils {
	public static String getContainerDataFieldGetterCodePhrase(
		String dataGetter,
		String index,
		LogicalType eleType) {
		switch (eleType.getTypeRoot()) {
			case INTEGER:
				return dataGetter + ".getInt(" + index + ")";
			case BIGINT:
				return dataGetter + ".getLong(" + index + ")";
			case FLOAT:
				return dataGetter + ".getFloat(" + index + ")";
			case DOUBLE:
				return dataGetter + ".getDouble(" + index + ")";
			case BOOLEAN:
				return dataGetter + ".getBoolean(" + index + ")";
			case VARCHAR:
			case CHAR:
				return dataGetter + ".getString(" + index + ")";
			case VARBINARY:
			case BINARY:
				return dataGetter + ".getBinary(" + index + ")";
			case ROW:
				int size = eleType.getChildren().size();
				return dataGetter + ".getRow(" + index + ", "
					+ size + ")";
			case MAP:
				return dataGetter + ".getMap(" + index + ")";
			case ARRAY:
				return dataGetter + ".getArray(" + index + ")";
			default:
				throw new IllegalArgumentException(
					"Unsupported data type in schema: " + eleType);
		}
	}

	public static String getTypeStrFromProto(
		Descriptors.FieldDescriptor fd, boolean isRepeated) throws PbCodegenException {
		String typeStr;
		switch (fd.getJavaType()) {
			case MESSAGE:
				if (fd.isMapField()) {
					//map
					Descriptors.FieldDescriptor keyFd = fd
						.getMessageType()
						.findFieldByName(PbConstant.PB_MAP_KEY_NAME);
					Descriptors.FieldDescriptor valueFd = fd.getMessageType()
						.findFieldByName(PbConstant.PB_MAP_VALUE_NAME);
					//key and value cannot be repeated
					String keyTypeStr = getTypeStrFromProto(keyFd, false);
					String valueTypeStr = getTypeStrFromProto(valueFd, false);
					typeStr = "Map<" + keyTypeStr + "," + valueTypeStr + ">";
				} else {
					//simple message
					typeStr = PbFormatUtils.getFullJavaName(fd.getMessageType());
				}
				break;
			case INT:
				typeStr = "Integer";
				break;
			case LONG:
				typeStr = "Long";
				break;
			case STRING:
				typeStr = "String";
				break;
			case ENUM:
				typeStr = PbFormatUtils.getFullJavaName(fd.getEnumType());
				break;
			case FLOAT:
				typeStr = "Float";
				break;
			case DOUBLE:
				typeStr = "Double";
				break;
			case BYTE_STRING:
				typeStr = "ByteString";
				break;
			case BOOLEAN:
				typeStr = "Boolean";
				break;
			default:
				throw new PbCodegenException(
					"do not support field type: " + fd.getJavaType());
		}
		if (isRepeated) {
			return "List<" + typeStr + ">";
		} else {
			return typeStr;
		}
	}

	public static String getTypeStrFromLogicType(LogicalType type) {
		switch (type.getTypeRoot()) {
			case INTEGER:
				return "int";
			case BIGINT:
				return "long";
			case FLOAT:
				return "float";
			case DOUBLE:
				return "double";
			case BOOLEAN:
				return "boolean";
			case VARCHAR:
			case CHAR:
				return "StringData";
			case VARBINARY:
			case BINARY:
				return "byte[]";
			case ROW:
				return "RowData";
			case MAP:
				return "MapData";
			case ARRAY:
				return "ArrayData";
			default:
				throw new IllegalArgumentException(
					"Unsupported data type in schema: " + type);
		}
	}

	public static String getDefaultPbValue(Descriptors.FieldDescriptor fieldDescriptor) throws PbCodegenException {
		switch (fieldDescriptor.getJavaType()) {
			case MESSAGE:
				return PbFormatUtils.getFullJavaName(fieldDescriptor.getMessageType())
					+ ".getDefaultInstance()";
			case INT:
				return "0";
			case LONG:
				return "0L";
			case STRING:
				return "\"\"";
			case ENUM:
				return PbFormatUtils.getFullJavaName(fieldDescriptor.getEnumType())
					+ ".values()[0]";
			case FLOAT:
				return "0.0f";
			case DOUBLE:
				return "0.0d";
			case BYTE_STRING:
				return "ByteString.EMPTY";
			case BOOLEAN:
				return "false";
			default:
				throw new PbCodegenException(
					"do not support field type: " + fieldDescriptor.getJavaType());
		}
	}

	public static String generateArrElementCodeWithDefaultValue(
		String arrDataVar,
		String iVar,
		String pbVar,
		String dataVar,
		Descriptors.FieldDescriptor elementFd,
		LogicalType elementType) throws PbCodegenException {
		PbCodegenAppender appender = new PbCodegenAppender();
		String protoTypeStr = PbCodegenUtils.getTypeStrFromProto(elementFd, false);
		String dataTypeStr = PbCodegenUtils.getTypeStrFromLogicType(elementType);
		appender.appendLine(protoTypeStr + " " + pbVar);
		appender.appendRawLine("if(" + arrDataVar + ".isNullAt(" + iVar + ")){");
		appender.appendLine(pbVar + "=" + PbCodegenUtils.getDefaultPbValue(elementFd));
		appender.appendRawLine("}else{");
		appender.appendLine(dataTypeStr + " " + dataVar);
		String getElementDataCode = PbCodegenUtils.getContainerDataFieldGetterCodePhrase(
			arrDataVar,
			iVar,
			elementType);
		appender.appendLine(dataVar + " = " + getElementDataCode);
		PbCodegenSerializer codegenDes = PbCodegenSerializeFactory.getPbCodegenSer(
			elementFd,
			elementType);
		String code = codegenDes.codegen(pbVar, dataVar);
		appender.appendSegment(code);
		appender.appendRawLine("}");
		return appender.code();
	}
}
