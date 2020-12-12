package org.apache.flink.formats.pb;

public class PbCodegenException extends Exception {
	public PbCodegenException() {
	}

	public PbCodegenException(String message) {
		super(message);
	}

	public PbCodegenException(String message, Throwable cause) {
		super(message, cause);
	}

	public PbCodegenException(Throwable cause) {
		super(cause);
	}

	public PbCodegenException(
		String message,
		Throwable cause,
		boolean enableSuppression,
		boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
