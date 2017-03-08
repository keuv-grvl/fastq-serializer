package fr.isima.fastxrecord;

import java.io.Serializable;

public abstract class FastxRecord implements Serializable {

	protected String sequenceHeader;
	protected String sequenceString;

	protected int length;

	public FastxRecord(String header, String sequence) {
		sequenceHeader = header;
		sequenceString = sequence;

		length = sequence.length();
	}

	public String getSequenceHeader() {
		return sequenceHeader;
	}

	public String getSequenceString() {
		return sequenceString;
	}

	public int getLength() {
		return length;
	}

}
