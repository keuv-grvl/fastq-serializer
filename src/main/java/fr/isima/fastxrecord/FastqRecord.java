package fr.isima.fastxrecord;

public class FastqRecord extends FastaRecord {

	protected String qualityHeader;
	protected String qualityString;

	protected int qualities[];

	protected int qualityValue;
	protected Encoding encoding;

	public FastqRecord(FastqRecord fqr) {
		super(fqr.getSequenceHeader(), fqr.getSequenceHeader());

		qualityHeader = fqr.getQualityHeader();
		qualityString = fqr.getQualityString();

		encoding = fqr.getEncoding();
		qualityValue = fqr.getMeanQuality();

		qualities = new int[length];
		System.arraycopy(fqr.getQualities(), 0, qualities, 0, length);
	}

	public FastqRecord(String header, String sequence, String qualHeader, String quality) {
		super(header, sequence);

		qualityHeader = qualHeader;
		qualityString = quality;

		// TODO Detect Encoding
		encoding = Encoding.SANGER;
		// qualityValue = encoding.getQualityValue(qualityString);
		qualityValue = 0;
		qualities = new int[length];
		for (int i = 0; i < length; i++) {
			int val = encoding.getQualityValue(qualityString.charAt(i));
			qualities[i] = val;
			qualityValue += val;
		}
	}

	public String getQualityHeader() {
		return qualityHeader;
	}

	public String getQualityString() {
		return qualityString;
	}

	public int getTotalQuality() {
		return qualityValue;
	}

	public int getMeanQuality() {
		return qualityValue / length;
	}

	public Encoding getEncoding() {
		return encoding;
	}

	// EN private pour que le jour où on aura besoin
	private int[] getQualities() {
		return qualities;
	}

	// gestion d'erreur assez pauvre
	public int getQualityAt(int pos) {
		int res = (pos >= 0 && pos < qualities.length) ? qualities[pos] : 0;
		return res;
	}
}
