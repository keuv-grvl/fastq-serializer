package fr.isima.fastqserializer;

import htsjdk.samtools.fastq.FastqRecord;

public class SequenceFileCommons {

	/**
	 * Returns the quality of a sequence object
	 * 
	 * @param fqrecord
	 *            : the sequence object
	 * @return the quality
	 */
	public int getSequenceQuality(FastqRecord fqrecord) {

		return getStringQuality(fqrecord.getBaseQualityString());
	}

	/**
	 * Returns the quality of a sequence quality
	 * 
	 * @param qualityString
	 *            : the string representing the quality of the sequence
	 * @return the total quality
	 */
	public int getStringQuality(String qualityString) {
		int res = 0;
		for (int i = 0; i < qualityString.length(); i++) {
			res += getQualityValue(qualityString.charAt(i));
		}
		return res;
	}

	/**
	 * Returns the integer value of a char
	 * 
	 * @param qualityChar
	 *            : the character representing a quality
	 * @return the intger value of that char
	 */
	public int getQualityValue(char qualityChar) {
		return qualityChar - '!';
	}

}
