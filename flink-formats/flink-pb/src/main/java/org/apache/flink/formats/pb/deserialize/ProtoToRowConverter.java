package org.apache.flink.formats.pb.deserialize;

import org.apache.flink.formats.pb.PbCodegenAppender;
import org.apache.flink.formats.pb.PbCodegenException;
import org.apache.flink.formats.pb.PbConstant;
import org.apache.flink.formats.pb.PbFormatUtils;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FileDescriptor.Syntax;
import org.codehaus.janino.ScriptEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProtoToRowConverter {
	private static final Logger LOG = LoggerFactory.getLogger(ProtoToRowConverter.class);
	private ScriptEvaluator se;
	private Method parseFromMethod;

	public ProtoToRowConverter(
		String messageClassName,
		RowType rowType,
		boolean ignoreDefaultValues) throws PbCodegenException {
		try {
			Descriptors.Descriptor descriptor = PbFormatUtils.getDescriptor(messageClassName);
			Class<?> messageClass = Class.forName(messageClassName);
			if (descriptor.getFile().getSyntax() == Syntax.PROTO3) {
				ignoreDefaultValues = false;
			}
			se = new ScriptEvaluator();
			se.setParameters(new String[]{"message"}, new Class[]{messageClass});
			se.setReturnType(RowData.class);
			se.setDefaultImports(
				RowData.class.getName(),
				ArrayData.class.getName(),
				StringData.class.getName(),
				GenericRowData.class.getName(),
				GenericMapData.class.getName(),
				GenericArrayData.class.getName(),
				ArrayList.class.getName(),
				List.class.getName(),
				Map.class.getName(),
				HashMap.class.getName());

			PbCodegenAppender codegenAppender = new PbCodegenAppender();
			codegenAppender.appendLine("RowData rowData=null");
			PbCodegenDeserializer codegenDes = PbCodegenDeserializeFactory.getPbCodegenTopRowDes(
				descriptor,
				rowType,
				ignoreDefaultValues);
			String genCode = codegenDes.codegen("rowData", "message");
			codegenAppender.appendSegment(genCode);
			codegenAppender.appendLine("return rowData");

			String printCode = codegenAppender.printWithLineNumber();
			LOG.debug("Protobuf decode codegen: \n" + printCode);

			se.cook(codegenAppender.code());
			parseFromMethod = messageClass.getMethod(PbConstant.PB_METHOD_PARSE_FROM, byte[].class);
		} catch (Exception ex) {
			throw new PbCodegenException(ex);
		}
	}

	public RowData convertProtoBinaryToRow(byte[] data) throws Exception {
		Object messageObj = parseFromMethod.invoke(null, data);
		return (RowData) se.evaluate(new Object[]{messageObj});
	}
}
