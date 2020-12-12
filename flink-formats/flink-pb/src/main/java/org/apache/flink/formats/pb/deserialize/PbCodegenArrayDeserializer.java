package org.apache.flink.formats.pb.deserialize;

import org.apache.flink.formats.pb.PbCodegenAppender;
import org.apache.flink.formats.pb.PbCodegenException;
import org.apache.flink.formats.pb.PbCodegenUtils;
import org.apache.flink.formats.pb.PbCodegenVarId;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.protobuf.Descriptors;

public class PbCodegenArrayDeserializer implements PbCodegenDeserializer {
	private Descriptors.FieldDescriptor fd;
	private LogicalType elementType;
	private boolean ignoreDefaultValues;
	private PbCodegenAppender appender = new PbCodegenAppender();

	public PbCodegenArrayDeserializer(
		Descriptors.FieldDescriptor fd,
		LogicalType elementType,
		boolean ignoreDefaultValues) {
		this.fd = fd;
		this.elementType = elementType;
		this.ignoreDefaultValues = ignoreDefaultValues;
	}

	@Override
	public String codegen(
		String returnVarName,
		String messageGetStr) throws PbCodegenException {
		PbCodegenVarId varUid = PbCodegenVarId.getInstance();
		int uid = varUid.getAndIncrement();
		String protoTypeStr = PbCodegenUtils.getTypeStrFromProto(fd, false);
		String listPbVar = "list" + uid;
		String newArrDataVar = "newArr" + uid;
		String subReturnDataVar = "subReturnVar" + uid;
		String iVar = "i" + uid;
		String subPbObjVar = "subObj" + uid;

		appender.appendLine(
			"List<" + protoTypeStr + "> " + listPbVar + "=" + messageGetStr);
		appender.appendLine("Object[] " + newArrDataVar + "= new " + "Object[" + listPbVar
			+ ".size()]");
		appender.appendRawLine(
			"for(int " + iVar + "=0;" + iVar + " < " + listPbVar + ".size(); "
				+ iVar + "++){");
		appender.appendLine("Object " + subReturnDataVar + " = null");
		appender.appendLine(
			protoTypeStr + " " + subPbObjVar + " = (" + protoTypeStr + ")" + listPbVar + ".get("
				+ iVar + ")");
		PbCodegenDeserializer codegenDes = PbCodegenDeserializeFactory.getPbCodegenDes(
			fd,
			elementType,
			ignoreDefaultValues);
		String code = codegenDes.codegen(subReturnDataVar, subPbObjVar);
		appender.appendSegment(code);
		appender.appendLine(newArrDataVar + "[" + iVar + "]=" + subReturnDataVar + "");
		appender.appendRawLine("}");
		appender.appendLine(returnVarName + " = new GenericArrayData(" + newArrDataVar + ")");
		return appender.code();
	}

}
