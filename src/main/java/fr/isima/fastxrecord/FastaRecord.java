package fr.isima.fastxrecord;

public class FastaRecord extends FastxRecord {

	public FastaRecord(FastaRecord far) {
		super(far.getSequenceHeader(), far.getSequenceString());
	}

	public FastaRecord(String header, String sequence) {
		super(header, sequence);
	}

}
