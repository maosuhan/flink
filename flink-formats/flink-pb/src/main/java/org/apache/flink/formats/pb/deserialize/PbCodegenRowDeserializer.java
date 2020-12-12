package org.apache.flink.formats.pb.deserialize;

import org.apache.flink.formats.pb.PbCodegenAppender;
import org.apache.flink.formats.pb.PbCodegenException;
import org.apache.flink.formats.pb.PbCodegenVarId;
import org.apache.flink.formats.pb.PbFormatUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;

import java.util.List;

public class PbCodegenRowDeserializer implements PbCodegenDeserializer {
	private List<Descriptors.FieldDescriptor> fds;
	private Descriptors.Descriptor descriptor;
	private RowType rowType;
	private boolean ignoreDefaultValues;
	private PbCodegenAppender appender = new PbCodegenAppender();

	public PbCodegenRowDeserializer(
		Descriptors.Descriptor descriptor,
		RowType rowType,
		boolean ignoreDefaultValues) {
		this.fds = descriptor.getFields();
		this.rowType = rowType;
		this.descriptor = descriptor;
		this.ignoreDefaultValues = ignoreDefaultValues;
	}

	@Override
	public String codegen(
		String returnVarName,
		String messageGetStr) throws PbCodegenException {
		PbCodegenVarId varUid = PbCodegenVarId.getInstance();
		int uid = varUid.getAndIncrement();
		String pbMessageVar = "message" + uid;
		String rowDataVar = "rowData" + uid;

		int fieldSize = rowType.getFieldNames().size();
		String pbMessageTypeStr = PbFormatUtils.getFullJavaName(descriptor);
		appender.appendLine(
			pbMessageTypeStr + " " + pbMessageVar + " = " + messageGetStr);
		appender.appendLine(
			"GenericRowData " + rowDataVar + " = new GenericRowData(" + fieldSize + ")");
		int index = 0;
		for (String fieldName : rowType.getFieldNames()) {
			int subUid = varUid.getAndIncrement();
			String elementDataVar = "elementDataVar" + subUid;

			LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
			Descriptors.FieldDescriptor elementFd = descriptor.findFieldByName(fieldName);
			String strongCamelFieldName = PbFormatUtils.getStrongCamelCaseJsonName(fieldName);
			PbCodegenDeserializer codegen = PbCodegenDeserializeFactory.getPbCodegenDes(
				elementFd,
				subType,
				ignoreDefaultValues);
			appender.appendLine("Object " + elementDataVar + " = null");
			if (ignoreDefaultValues) {
				//only works in syntax=proto2 and ignoreDefaultValues=true
				String isMessageNonEmptyStr = isMessageNonEmptyStr(
					pbMessageVar,
					strongCamelFieldName,
					elementFd);
				//ignoreDefaultValues must be false in pb3 mode or compilation error will occur
				appender.appendRawLine("if(" + isMessageNonEmptyStr + "){");
			}
			String elementMessageGetStr = pbMessageElementGetStr(
				pbMessageVar,
				strongCamelFieldName,
				elementFd);
			if (!elementFd.isRepeated()) {
				//field is not map or array
				//this step is needed to convert primitive type to boxed type to unify the object interface
				switch (elementFd.getJavaType()) {
					case INT:
						elementMessageGetStr = "Integer.valueOf(" + elementMessageGetStr + ")";
						break;
					case LONG:
						elementMessageGetStr = "Long.valueOf(" + elementMessageGetStr + ")";
						break;
					case FLOAT:
						elementMessageGetStr = "Float.valueOf(" + elementMessageGetStr + ")";
						break;
					case DOUBLE:
						elementMessageGetStr = "Double.valueOf(" + elementMessageGetStr + ")";
						break;
					case BOOLEAN:
						elementMessageGetStr = "Boolean.valueOf(" + elementMessageGetStr + ")";
						break;
				}
			}

			String code = codegen.codegen(elementDataVar, elementMessageGetStr);
			appender.appendSegment(code);
			if (ignoreDefaultValues) {
				appender.appendRawLine("}");
			}
			appender.appendLine(
				rowDataVar + ".setField(" + index + ", " + elementDataVar + ")");
			index += 1;
		}
		appender.appendLine(returnVarName + " = " + rowDataVar);
		return appender.code();
	}

	private String pbMessageElementGetStr(
		String message,
		String fieldName,
		Descriptors.FieldDescriptor fd) {
		if (fd.isMapField()) {
			return message + ".get" + fieldName + "Map()";
		} else if (fd.isRepeated()) {
			return message + ".get" + fieldName + "List()";
		} else {
			return message + ".get" + fieldName + "()";
		}
	}

	private String isMessageNonEmptyStr(
		String message,
		String fieldName,
		Descriptors.FieldDescriptor fd) {
		if (fd.isRepeated()) {
			return message + ".get" + fieldName + "Count() > 0";
		} else {
			// Proto3 syntax class do not have hasXXX interface.
			return message + ".has" + fieldName + "()";
		}
	}
}
