package org.apache.flink.formats.pb.serialize;

import org.apache.flink.formats.pb.PbCodegenException;

public interface PbCodegenSerializer {
	String codegen(String returnVarName, String rowFieldGetStr) throws PbCodegenException;
}
