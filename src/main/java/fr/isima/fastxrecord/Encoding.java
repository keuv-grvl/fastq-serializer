package fr.isima.fastxrecord;

public enum Encoding {
	SANGER('!',0),
	SOLEXA(':',-5) ,
	ILLUMINA('?',0); // A voir avec Kévin la dernière version du Illumina
	
	private char debutChar;
	private int debutVal;
	
	Encoding(char min, int val){
		debutChar = min;
		debutVal = val;
	}
	
	
	/*
	 * Returns the quality of a sequence quality 
	 * 
	 * @params qualityString : the string representing the quality of the sequence
	 * @return the total quality
	 */
	public int getQualityValue(String qualityString){
		int res = 0;
		for(int i = 0; i < qualityString.length(); i++){
			res += getQualityValue(qualityString.charAt(i));
		}
		return res;
	}
	
	/*
	 * Returns the integer value of a char
	 * 
	 * @params qualityChar : the character representing a quality
	 * @return the intger value of that char
	 */
	public int getQualityValue(char qualityChar){
		return (int) qualityChar - (int) debutChar + debutVal;
	}
}

