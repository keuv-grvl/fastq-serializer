package fr.isima.fastqserializer;

import htsjdk.samtools.fastq.FastqReader;
import htsjdk.samtools.fastq.FastqRecord;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SequenceFileManager {

	/*
	 * Reads a file containing the sequences' data 
	 * and constructs a List containing objects representing that Data
	 * 
	 * @params filepath : path to the file
	 * @return list containing object representing the sequence data
	 */
	private List<FastqRecord> getFqArray(String filePath){
		FastqReader fastqReader = new FastqReader(new File(filePath));
		List<FastqRecord> fastqArray = new ArrayList<FastqRecord>();

		for (FastqRecord fastq : fastqReader)
		{
			fastqArray.add(fastq);

		}
		fastqReader.close();
		return fastqArray;
	}
	
	/*
	 * Reads a file and prints the information read for each sequence
	 * @params filePath: path of the file to read
	 */
	public void readFastqFile(String filePath) throws IOException {
		FastqReader fastqReader = new FastqReader(new File(filePath));

		for (FastqRecord fastq : fastqReader)
		{
			System.out.println("Sequence: " + fastq.toString());
			System.out.println("\t getBaseQualityHeader: " + fastq.getBaseQualityHeader());
			System.out.println("\t getBaseQualityString: " + fastq.getBaseQualityString());
			//System.out.println("\t  	value: " + getStringQuality(fastq.getBaseQualityString()) );
			System.out.println("\t getReadHeader: " + fastq.getReadHeader());
			System.out.println("\t getReadString: " + fastq.getReadString());
			System.out.println("\t  	hashCode: " + fastq.hashCode());
			System.out.println("\t  	length: " + fastq.length());
			System.out.println("------------------------------- ");

		}
		fastqReader.close();
	}
	
	/*
	 * Converts a sequence file .fq to JavaRDD folder .fqrdd
	 * 
	 * @params sc: SparkContext in which we're operating
	 * @params filePath:  path oto the file we would like to convert (.fq)
	 * @params resultFolder: path to the newly created folder
	 */
	public void convertFastqToFqrdd(JavaSparkContext sc, String filePath, String resultFolder) throws IOException {
		
		List<FastqRecord> fastqArray = getFqArray(filePath);
		
		/* Transformation des objets fastq en objets RDD */
		System.out.println("Parallelisation");
		JavaRDD<FastqRecord> fastqRDD =  sc.parallelize(fastqArray);	

		System.out.println("Exportation");
		// si le dossier existe déjà, il lance une erreur
		fastqRDD.saveAsObjectFile(resultFolder );
		
		// using bzip2 to compress the output
		/* OutputStream fout = new org.apache.hadoop.io.compress.BZip2Codec()
							.createOutputStream(new FileOutputStream(resultFolder) );
		
		 *Il faut ensuite écrire les bits...ou écire les objets
		 * https://www.mkyong.com/java/how-to-compress-serialized-object-into-file/
		 */
	}

	
	/*
	 * Reads a folder and searches for the files containing Object data and
	 * makes a JavaRDD of containing objects representing the data read
	 * those files are the result of a previous Spark operation
	 * name format: part#####  ex: part-00000
	 * the folder must also contain an empty file with the name _SUCCESS
	 * 
	 * @params sc: Spark Context in which we're operating
	 * @params folderPath: path to the folder which we want to read
	 * @return JavaRDD containing objects with the data read
	 */
	private  JavaRDD<FastqRecord> readFqRDDFolder(JavaSparkContext sc, String folderPath) throws IOException{	

		JavaRDD<FastqRecord> fqrdd = sc.emptyRDD() ;
		File folder = new File(folderPath);
		File[] listOfFiles ; //= folder.listFiles();
		
		System.out.println(folderPath);
		// create new filename filter
        FilenameFilter fileNameFilter = new FilenameFilter() {
  
           public boolean accept(File dir, String name) {
              if(name.lastIndexOf('-')>0)
              {
                 // get last index for '-' char
            	  
                 int lastIndex = name.lastIndexOf('-');
                 
                 // get extension
                 String str = name.substring(0,lastIndex);
                 
                 // match path name extension
                 if(str.equals("part"))
                 {
                    return true;
                 }
              }
              return false;
           }
        };
		
     // returns pathnames for files and directory
        listOfFiles = folder.listFiles(fileNameFilter);
     // for each pathname in pathname array
        for(File path : listOfFiles){
        	System.out.println(path.toString());
        	JavaRDD<FastqRecord> temp = sc.objectFile(path.toString()); 
        	fqrdd = temp.union(fqrdd);
        }
		return fqrdd; 	
		
	}
	
	
	public void filterFqRdd(JavaSparkContext sc, String folderPath,
			final int minLength,final int maxLength, final int minQual, final int maxQual, boolean atgc) throws IOException{
		JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc,folderPath);
		
		SequenceFileEditor.filterJavaRdd();
		
		
	}
	
	// with ID: il faudrait qu'une fonction qui gère with id or not (si c'est NULL or pas)
	public void filterFqRddwID(JavaSparkContext sc, String folderPath, String idFilePath,
			final int minLength,final int maxLength, final int minQual, final int maxQual, boolean atgc) throws IOException{
		//JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc,folderPath);
			
		File headerFile = new File(idFilePath);
		final List<String> headers = Files.readAllLines(FileSystems.getDefault().getPath(idFilePath));
		
		JavaRDD<FastqRecord>  fqrdd = filterFqRdd(sc, folderPath, minLength, maxLength, minQual, maxQual, atgc);
		JavaRDD<FastqRecord>  fltrRes = fqrdd.filter(new Function<FastqRecord,Boolean> (){
			public Boolean call (FastqRecord fq ){
				return headers.contains(fq.getReadHeader());
				
			}
		});
		
		
		// return fltrRes;
		
	}
	
}
}
