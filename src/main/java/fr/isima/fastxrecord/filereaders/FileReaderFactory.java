package fr.isima.fastxrecord.filereaders;

public class FileReaderFactory {
	public static FastxFileReader getFileReader(String filepath) {
		// TODO Detect the file extension or the format of the strings inside to
		// determine the format of the data
		return new FastqFileReader(filepath);
	}
}
