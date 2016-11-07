package fr.isima.fastqserializer;

import org.biojava.nbio.sequencing.io.fastq.*;

import java.io.File;
import java.io.IOException;

public class FastqFileManager {
	
	public void readFastqFile(String filePath) throws IOException {
		FastqReader fastqReader = new SangerFastqReader();
		
		for (Fastq fastq : fastqReader.read(new File(filePath)))
    	{
			System.out.println("Sequence Description: " + fastq.getDescription());
			 System.out.println("\t Quality: " + fastq.getQuality());
			 System.out.println("\t Sequence length: " + fastq.getSequence().length());
			 System.out.println("\t Sequence: " + fastq.getSequence());
			 System.out.println("\t Variant: " + fastq.getVariant());
			 System.out.println("------------------------------- ");
			
    	}
	}

}
