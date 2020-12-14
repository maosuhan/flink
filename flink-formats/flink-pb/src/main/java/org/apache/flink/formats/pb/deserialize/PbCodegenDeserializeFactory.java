package org.apache.flink.formats.pb.deserialize;

import org.apache.flink.formats.pb.PbCodegenException;
import org.apache.flink.formats.pb.PbFormatUtils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;

public class PbCodegenDeserializeFactory {
	public static PbCodegenDeserializer getPbCodegenDes(
		Descriptors.FieldDescriptor fd,
		LogicalType type,
		boolean readDefaultValues) throws PbCodegenException {
		if (type instanceof RowType) {
			return new PbCodegenRowDeserializer(fd.getMessageType(), (RowType) type,
				readDefaultValues);
		} else if (PbFormatUtils.isSimpleType(type)) {
			return new PbCodegenSimpleDeserializer(fd);
		} else if (type instanceof ArrayType) {
			return new PbCodegenArrayDeserializer(
				fd, ((ArrayType) type).getElementType(), readDefaultValues);
		} else if (type instanceof MapType) {
			return new PbCodegenMapDeserializer(fd, (MapType) type, readDefaultValues);
		} else {
			throw new PbCodegenException("cannot support flink type: " + type);
		}
	}

	public static PbCodegenDeserializer getPbCodegenTopRowDes(
		Descriptors.Descriptor descriptor,
		RowType rowType,
		boolean readDefaultValues) {
		return new PbCodegenRowDeserializer(descriptor, rowType, readDefaultValues);
	}
}
