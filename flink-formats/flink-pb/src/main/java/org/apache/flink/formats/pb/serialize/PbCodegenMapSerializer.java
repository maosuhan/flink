package org.apache.flink.formats.pb.serialize;

import org.apache.flink.formats.pb.PbCodegenAppender;
import org.apache.flink.formats.pb.PbCodegenUtils;
import org.apache.flink.formats.pb.PbCodegenVarId;
import org.apache.flink.formats.pb.PbConstant;
import org.apache.flink.formats.pb.PbCodegenException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import com.google.protobuf.Descriptors;

public class PbCodegenMapSerializer implements PbCodegenSerializer {
	private Descriptors.FieldDescriptor fd;
	private MapType mapType;

	public PbCodegenMapSerializer(
		Descriptors.FieldDescriptor fd,
		MapType mapType) {
		this.fd = fd;
		this.mapType = mapType;
	}

	@Override
	public String codegen(String returnVarName, String rowFieldGetStr) throws PbCodegenException {
		PbCodegenVarId varUid = PbCodegenVarId.getInstance();
		int uid = varUid.getAndIncrement();
		LogicalType keyType = mapType.getKeyType();
		LogicalType valueType = mapType.getValueType();
		Descriptors.FieldDescriptor keyFd = fd
			.getMessageType()
			.findFieldByName(PbConstant.PB_MAP_KEY_NAME);
		Descriptors.FieldDescriptor valueFd = fd
			.getMessageType()
			.findFieldByName(PbConstant.PB_MAP_VALUE_NAME);

		PbCodegenAppender appender = new PbCodegenAppender();
		String keyProtoTypeStr = PbCodegenUtils.getTypeStrFromProto(keyFd, false);
		String valueProtoTypeStr = PbCodegenUtils.getTypeStrFromProto(valueFd, false);

		String keyArrDataVar = "keyArrData" + uid;
		String valueArrDataVar = "valueArrData" + uid;
		String iVar = "i" + uid;
		String pbMapVar = "resultPbMap" + uid;
		String keyPbVar = "keyPbVar" + uid;
		String valuePbVar = "valuePbVar" + uid;
		String keyDataVar = "keyDataVar" + uid;
		String valueDataVar = "valueDataVar" + uid;

		appender.appendLine("ArrayData " + keyArrDataVar + " = " + rowFieldGetStr + ".keyArray()");
		appender.appendLine(
			"ArrayData " + valueArrDataVar + " = " + rowFieldGetStr + ".valueArray()");

		appender.appendLine("Map<" + keyProtoTypeStr + ", " + valueProtoTypeStr + "> " + pbMapVar
			+ " = new HashMap()");
		appender.appendRawLine("for(int " + iVar + " = 0; " + iVar + " < " + keyArrDataVar
			+ ".size(); " + iVar + "++){");

		//process key
		String keyGenCode = PbCodegenUtils.generateArrElementCodeWithDefaultValue(
			keyArrDataVar,
			iVar,
			keyPbVar,
			keyDataVar,
			keyFd,
			keyType);
		appender.appendSegment(keyGenCode);

		//process value
		String valueGenCode = PbCodegenUtils.generateArrElementCodeWithDefaultValue(
			valueArrDataVar,
			iVar,
			valuePbVar,
			valueDataVar,
			valueFd,
			valueType);
		appender.appendSegment(valueGenCode);

		appender.appendLine(pbMapVar + ".put(" + keyPbVar + ", " + valuePbVar + ")");
		appender.appendRawLine("}");

		appender.appendLine(returnVarName + " = " + pbMapVar);
		return appender.code();
	}

}
