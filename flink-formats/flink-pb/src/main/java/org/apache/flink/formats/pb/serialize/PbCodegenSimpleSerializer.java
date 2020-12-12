package org.apache.flink.formats.pb.serialize;

import org.apache.flink.formats.pb.PbCodegenAppender;
import org.apache.flink.formats.pb.PbCodegenVarId;
import org.apache.flink.formats.pb.PbFormatUtils;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class PbCodegenSimpleSerializer implements PbCodegenSerializer {
	private Descriptors.FieldDescriptor fd;
	private LogicalType type;

	public PbCodegenSimpleSerializer(
		Descriptors.FieldDescriptor fd,
		LogicalType type) {
		this.fd = fd;
		this.type = type;
	}

	/**
	 * @param returnPbVar
	 * @param dataGetStr dataGetStr is the expression string from row data, the real
	 * 	value of rowFieldGetStr may be null, String, int, long, double, float, boolean, byte[]
	 *
	 * @return
	 */
	@Override
	public String codegen(String returnPbVar, String dataGetStr) {
		switch (type.getTypeRoot()) {
			case INTEGER:
			case BIGINT:
			case FLOAT:
			case DOUBLE:
			case BOOLEAN:
				return returnPbVar + " = " + dataGetStr + ";";
			case VARCHAR:
			case CHAR:
				PbCodegenAppender appender = new PbCodegenAppender();
				int uid = PbCodegenVarId.getInstance().getAndIncrement();
				String fromVar = "fromVar" + uid;
				appender.appendLine("String " + fromVar);
				appender.appendLine(fromVar + " = " + dataGetStr + ".toString()");
				if (fd.getJavaType() == JavaType.ENUM) {
					String enumValueDescVar = "enumValueDesc" + uid;
					String enumTypeStr = PbFormatUtils.getFullJavaName(fd.getEnumType());
					appender.appendLine(
						"Descriptors.EnumValueDescriptor " + enumValueDescVar + "=" + enumTypeStr
							+ ".getDescriptor().findValueByName(" + fromVar + ")");
					appender.appendRawLine("if(null == " + enumValueDescVar + "){");
					appender.appendLine(returnPbVar + " = " + enumTypeStr + ".values()[0]");
					appender.appendRawLine("}");
					appender.appendRawLine("else{");
					appender.appendLine(
						returnPbVar + " = " + enumTypeStr + ".valueOf(" + enumValueDescVar + ")");
					appender.appendLine("}");
				} else {
					appender.appendLine(returnPbVar + " = " + fromVar);
				}
				return appender.code();
			case VARBINARY:
			case BINARY:
				return returnPbVar + " = ByteString.copyFrom(" + dataGetStr + ");";
			default:
				throw new IllegalArgumentException(
					"Unsupported data type in schema: " + type);
		}
	}
}
