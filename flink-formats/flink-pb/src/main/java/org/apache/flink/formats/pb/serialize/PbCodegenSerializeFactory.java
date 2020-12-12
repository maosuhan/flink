package org.apache.flink.formats.pb.serialize;

import org.apache.flink.formats.pb.PbFormatUtils;
import org.apache.flink.formats.pb.PbCodegenException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;

public class PbCodegenSerializeFactory {
	public static PbCodegenSerializer getPbCodegenSer(
		Descriptors.FieldDescriptor fd,
		LogicalType type) throws PbCodegenException {
		if (type instanceof RowType) {
			return new PbCodegenRowSerializer(fd.getMessageType(), (RowType) type);
		} else if (PbFormatUtils.isSimpleType(type)) {
			return new PbCodegenSimpleSerializer(fd, type);
		} else if (type instanceof ArrayType) {
			return new PbCodegenArraySerializer(
				fd,
				((ArrayType) type).getElementType());
		} else if (type instanceof MapType) {
			return new PbCodegenMapSerializer(fd, (MapType) type);
		} else {
			throw new PbCodegenException("Cannot support flink data type: " + type);
		}
	}

	public static PbCodegenSerializer getPbCodegenTopRowSer(
		Descriptors.Descriptor descriptor,
		RowType rowType) {
		return new PbCodegenRowSerializer(descriptor, rowType);
	}
}
