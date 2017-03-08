package fr.isima.fastxrecord.filereaders;

import java.io.IOException;
import java.util.Vector;

import fr.isima.fastxrecord.FastxRecord;

public abstract class FastxFileReader {
	protected String filepath;

	public FastxFileReader(String filep) {
		filepath = filep;
	}

	public abstract Vector<FastxRecord> readFile() throws IOException;
}
