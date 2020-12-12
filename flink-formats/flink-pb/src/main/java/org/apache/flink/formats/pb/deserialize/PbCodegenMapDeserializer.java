package org.apache.flink.formats.pb.deserialize;

import org.apache.flink.formats.pb.PbCodegenAppender;
import org.apache.flink.formats.pb.PbCodegenException;
import org.apache.flink.formats.pb.PbCodegenUtils;
import org.apache.flink.formats.pb.PbCodegenVarId;
import org.apache.flink.formats.pb.PbConstant;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import com.google.protobuf.Descriptors;

public class PbCodegenMapDeserializer implements PbCodegenDeserializer {
	private Descriptors.FieldDescriptor fd;
	private MapType mapType;
	private boolean ignoreDefaultValues;

	public PbCodegenMapDeserializer(
		Descriptors.FieldDescriptor fd,
		MapType mapType,
		boolean ignoreDefaultValues) {
		this.fd = fd;
		this.mapType = mapType;
		this.ignoreDefaultValues = ignoreDefaultValues;
	}

	@Override
	public String codegen(
		String returnVarName,
		String messageGetStr) throws PbCodegenException {
		PbCodegenVarId varUid = PbCodegenVarId.getInstance();
		int uid = varUid.getAndIncrement();

		LogicalType keyType = mapType.getKeyType();
		LogicalType valueType = mapType.getValueType();
		Descriptors.FieldDescriptor keyFd = fd.getMessageType()
			.findFieldByName(PbConstant.PB_MAP_KEY_NAME);
		Descriptors.FieldDescriptor valueFd = fd.getMessageType()
			.findFieldByName(PbConstant.PB_MAP_VALUE_NAME);

		PbCodegenAppender appender = new PbCodegenAppender();
		String pbKeyTypeStr = PbCodegenUtils.getTypeStrFromProto(keyFd, false);
		String pbValueTypeStr = PbCodegenUtils.getTypeStrFromProto(valueFd, false);
		String pbMapVar = "pbMap" + uid;
		String pbMapEntryVar = "pbEntry" + uid;
		String resultDataMapVar = "resultDataMap" + uid;
		String keyDataVar = "keyDataVar" + uid;
		String valueDataVar = "valueDataVar" + uid;

		appender.appendLine("Map<" + pbKeyTypeStr + "," + pbValueTypeStr + "> " + pbMapVar + " = "
			+ messageGetStr + ";");
		appender.appendLine("Map " + resultDataMapVar + " = new HashMap()");
		appender.appendRawLine(
			"for(Map.Entry<" + pbKeyTypeStr + "," + pbValueTypeStr + "> " + pbMapEntryVar + ": "
				+ pbMapVar + ".entrySet()){");
		appender.appendLine("Object " + keyDataVar + "= null");
		appender.appendLine("Object " + valueDataVar + "= null");
		PbCodegenDeserializer keyDes = PbCodegenDeserializeFactory.getPbCodegenDes(
			keyFd,
			keyType,
			ignoreDefaultValues);
		PbCodegenDeserializer valueDes = PbCodegenDeserializeFactory.getPbCodegenDes(
			valueFd,
			valueType,
			ignoreDefaultValues);
		String keyGenCode = keyDes.codegen(
			keyDataVar,
			"((" + pbKeyTypeStr + ")" + pbMapEntryVar + ".getKey())");
		appender.appendSegment(keyGenCode);
		String valueGenCode = valueDes.codegen(
			valueDataVar,
			"((" + pbValueTypeStr + ")" + pbMapEntryVar + ".getValue())");
		appender.appendSegment(valueGenCode);
		appender.appendLine(resultDataMapVar + ".put(" + keyDataVar + ", " + valueDataVar + ")");
		appender.appendRawLine("}");
		appender.appendLine(returnVarName + " = new GenericMapData(" + resultDataMapVar + ")");
		return appender.code();
	}

}
