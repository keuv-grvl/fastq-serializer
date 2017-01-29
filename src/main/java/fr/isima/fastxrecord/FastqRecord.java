package fr.isima.fastxrecord;

public class FastqRecord extends FastaRecord {
	
	
	protected String qualityHeader;
	protected String qualityString;
	
	protected int qualityValue;
	protected Encoding encoding;
	
	
	public FastqRecord(FastqRecord fqr) {
		super(fqr.getSequenceHeader(), fqr.getSequenceHeader());
		
		qualityHeader = fqr.getQualityHeader();
		qualityString = fqr.getQualityString();
		
		encoding = fqr.getEncoding();
		qualityValue = fqr.getQualityValue();
	}

	public FastqRecord(String header, String sequence, String qualHeader, String quality){
		super(header,sequence);
		
		qualityHeader = qualHeader;
		qualityString = quality;
		
		// TODO Detect Encoding
		encoding = Encoding.SANGER;
		qualityValue = encoding.getQualityValue(qualityString);
	}

	public String getQualityHeader(){
		return qualityHeader;
	}
	public String getQualityString(){
		return qualityString;
	}
	
	public int getQualityValue(){
		return qualityValue / length;
	}
	public Encoding getEncoding(){
		return encoding;
	}
}
