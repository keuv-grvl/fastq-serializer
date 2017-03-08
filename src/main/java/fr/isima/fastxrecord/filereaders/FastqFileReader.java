package fr.isima.fastxrecord.filereaders;

import java.io.FileInputStream;

import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;

import fr.isima.fastxrecord.FastqRecord;
import fr.isima.fastxrecord.FastxRecord;

public class FastqFileReader extends FastxFileReader {

	public FastqFileReader(String filepath) {
		super(filepath);
	}
	
	@Override
	public Vector<FastxRecord> readFile() throws IOException {
		
		Vector<FastxRecord> res = new Vector<FastxRecord>(); 
		
		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
		    inputStream = new FileInputStream(filepath);
		    sc = new Scanner(inputStream, "UTF-8");
		    while (sc.hasNextLine()) {
		    	
		        String fheader = sc.nextLine(); // lecture de la première en-tête
		        String sequence = sc.nextLine(); // lecture de la séquence
		        String sheader = sc.nextLine(); // lecture de la deuxième en-tête
		        String quality = sc.nextLine(); // lecture de la qualité
		        res.add(new FastqRecord(fheader,sequence,sheader,quality));
		        // System.out.println(line);
		    }
		    // note that Scanner suppresses exceptions
		    if (sc.ioException() != null) {
		        throw sc.ioException();
		    }
		} 
		finally {
		    if (inputStream != null) {
		        inputStream.close();
		    }
		    if (sc != null) {
		        sc.close();
		    }
		}
		
		return res;
	}

}
