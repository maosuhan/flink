package org.apache.flink.formats.pb;

import java.util.concurrent.atomic.AtomicInteger;

public class PbCodegenVarId {
	private static PbCodegenVarId varUid = new PbCodegenVarId();
	private AtomicInteger atomicInteger = new AtomicInteger();

	private PbCodegenVarId() {
	}

	public static PbCodegenVarId getInstance() {
		return varUid;
	}

	public int getAndIncrement() {
		return atomicInteger.getAndIncrement();
	}
}
