package org.apache.flink.formats.pb;

import java.util.Arrays;

public class PbCodegenAppender {
	private StringBuilder sb;

	public PbCodegenAppender() {
		sb = new StringBuilder();
	}

	public void appendSegment(String code) {
		sb.append(code + "\n");
	}

	public void appendLine(String code) {
		sb.append(code + ";\n");
	}

	public void appendRawLine(String code) {
		sb.append(code + "\n");
	}

	public String code() {
		return sb.toString();
	}

	public static String printWithLineNumber(String code) {
		StringBuilder sb = new StringBuilder();
		String[] lines = code.split("\n");
		for (int i = 0; i < lines.length; i++) {
			sb.append("Line " + (i + 1) + ": " + lines[i] + "\n");
		}
		return sb.toString();
	}

	public String printWithLineNumber() {
		StringBuilder newSb = new StringBuilder();
		String[] lines = sb.toString().split("\n");
		for (int i = 0; i < lines.length; i++) {
			newSb.append("Line " + (i + 1) + ": " + lines[i] + "\n");
		}
		return newSb.toString();
	}
}
