package org.apache.flink.formats.pb.serialize;

import org.apache.flink.formats.pb.PbCodegenAppender;
import org.apache.flink.formats.pb.PbCodegenUtils;
import org.apache.flink.formats.pb.PbCodegenVarId;
import org.apache.flink.formats.pb.PbFormatUtils;
import org.apache.flink.formats.pb.PbCodegenException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;

import java.util.List;

public class PbCodegenRowSerializer implements PbCodegenSerializer {
	private List<Descriptors.FieldDescriptor> fds;
	private Descriptors.Descriptor descriptor;
	private RowType rowType;

	public PbCodegenRowSerializer(
		Descriptors.Descriptor descriptor,
		RowType rowType) {
		this.fds = descriptor.getFields();
		this.rowType = rowType;
		this.descriptor = descriptor;
	}

	@Override
	public String codegen(String returnVarName, String rowFieldGetStr) throws PbCodegenException {
		PbCodegenVarId varUid = PbCodegenVarId.getInstance();
		int uid = varUid.getAndIncrement();
		PbCodegenAppender appender = new PbCodegenAppender();
		String rowDataVar = "rowData" + uid;
		String pbMessageTypeStr = PbFormatUtils.getFullJavaName(descriptor);
		String messageBuilderVar = "messageBuilder" + uid;
		appender.appendLine("RowData " + rowDataVar + " = " + rowFieldGetStr);
		appender.appendLine(pbMessageTypeStr + ".Builder " + messageBuilderVar + " = "
			+ pbMessageTypeStr + ".newBuilder()");
		int index = 0;
		for (String fieldName : rowType.getFieldNames()) {
			Descriptors.FieldDescriptor elementFd = descriptor.findFieldByName(fieldName);
			LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
			int subUid = varUid.getAndIncrement();
			String elementPbVar = "elementPbVar" + subUid;
			String elementPbTypeStr;
			if (elementFd.isMapField()) {
				elementPbTypeStr = PbCodegenUtils.getTypeStrFromProto(elementFd, false);
			} else {
				elementPbTypeStr = PbCodegenUtils.getTypeStrFromProto(
					elementFd,
					elementFd.isRepeated());
			}
			String strongCamelFieldName = PbFormatUtils.getStrongCamelCaseJsonName(fieldName);

			appender.appendRawLine("if(!" + rowDataVar + ".isNullAt(" + index + ")){");
			appender.appendLine(elementPbTypeStr + " " + elementPbVar);
			String subRowGetCode = PbCodegenUtils.getContainerDataFieldGetterCodePhrase(
				rowDataVar, index + "", subType
			);
			PbCodegenSerializer codegen = PbCodegenSerializeFactory.getPbCodegenSer(
				elementFd,
				subType);
			String code = codegen.codegen(elementPbVar, subRowGetCode);
			appender.appendSegment(code);
			if (subType.getTypeRoot() == LogicalTypeRoot.ARRAY) {
				appender.appendLine(
					messageBuilderVar + ".addAll" + strongCamelFieldName + "(" + elementPbVar
						+ ")");
			} else if (subType.getTypeRoot() == LogicalTypeRoot.MAP) {
				appender.appendLine(
					messageBuilderVar + ".putAll" + strongCamelFieldName + "(" + elementPbVar
						+ ")");
			} else {
				appender.appendLine(
					messageBuilderVar + ".set" + strongCamelFieldName + "(" + elementPbVar
						+ ")");
			}
			appender.appendRawLine("}");
			index += 1;
		}
		appender.appendLine(returnVarName + " = " + messageBuilderVar + ".build()");
		return appender.code();
	}
}
