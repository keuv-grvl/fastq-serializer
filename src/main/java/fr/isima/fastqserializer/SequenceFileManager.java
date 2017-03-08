package fr.isima.fastqserializer;

import fr.isima.entities.KmeanMatrix;
import fr.isima.fastxrecord.*;
import fr.isima.fastxrecord.filereaders.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import scala.Tuple2;

public class SequenceFileManager {

	SequenceFileInterrogator fqInterrogator = new SequenceFileInterrogator();
	SequenceFileEditor	fqEditor = new SequenceFileEditor();
	
	public void getFqRDDSatistics(JavaSparkContext sc, String folderPath) throws IOException{
		System.out.println("=== Statistiques ===");
		JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc,folderPath);
		
		fqInterrogator.getFqRDDSatistics(fqrdd);
	}
	
	/*
	 * Reads a file containing the sequences' data 
	 * and constructs a List containing objects representing that Data
	 * 
	 * @params filepath : path to the file
	 * @return list containing object representing the sequence data
	 */
	private List<FastxRecord> getFqArray(String filePath) throws IOException{
		FastxFileReader fastqReader = FileReaderFactory.getFileReader(filePath);
		
		
		
		List<FastxRecord> fastqArray = new ArrayList<FastxRecord>();

		for (FastxRecord fastq : fastqReader.readFile())
		{
			fastqArray.add(fastq);

		}
		return fastqArray;
	}
	
	/*
	 * Reads a file and prints the information read for each sequence
	 * @params filePath: path of the file to read
	 */
	public void readFastqFile(String filePath) throws IOException {
		FastxFileReader fastqReader = FileReaderFactory.getFileReader(filePath);

		for (FastxRecord fastq : fastqReader.readFile())
		{
			System.out.println("Sequence: " + fastq.toString());
			System.out.println("\t getQualityHeader: " + ((FastqRecord) fastq).getQualityHeader());
			System.out.println("\t getQualityString: " + ((FastqRecord) fastq).getQualityString());
			System.out.println("\t  	value: " + ((FastqRecord) fastq).getMeanQuality());
			System.out.println("\t getReadHeader: " + ((FastqRecord) fastq).getSequenceHeader());
			System.out.println("\t getReadString: " + ((FastqRecord) fastq).getSequenceString());
			System.out.println("\t  	hashCode: " + ((FastqRecord) fastq).hashCode());
			System.out.println("\t  	length: " + ((FastqRecord) fastq).getLength());
			System.out.println("------------------------------- ");

		}
	}
	
	/*
	 * Converts a sequence file .fq to JavaRDD folder .fqrdd
	 * 
	 * @params sc: SparkContext in which we're operating
	 * @params filePath:  path oto the file we would like to convert (.fq)
	 * @params resultFolder: path to the newly created folder
	 */
	public void convertFastqToFqrdd(JavaSparkContext sc, String filePath, String resultFolder) throws IOException {
		
		List<FastxRecord> fastqArray = getFqArray(filePath);
		
		/* Transformation des objets fastq en objets RDD */
		System.out.println("Parallelisation");
		JavaRDD<FastxRecord> fastqRDD =  sc.parallelize(fastqArray);	

		System.out.println("Exportation");
		// si le dossier existe déjà, il lance une erreur
		fastqRDD.saveAsObjectFile(resultFolder);
		
		/*
		JavaPairRDD<FastxRecord,String> fqrdd = fastqRDD.mapToPair( new PairFunction<FastxRecord, FastxRecord, String>(){
			public Tuple2<FastxRecord,String> call(FastxRecord fq){
				
				return new Tuple2<FastxRecord,String>(fq,"");
			}
 		});
 		//*/
			
	
		//fqrdd.saveAsHadoopFile(resultFolder, FastxRecord.class, String.class, org.apache.hadoop.mapred.TextOutputFormat<> , org.apache.hadoop.io.compress.BZip2Codec.class);
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
	
	/*
	 * Reads a FqRdd folder and then filters it according to the criterias specified
	 * @params sc: Spark Context in which we're operating
	 * @params folderPath: Path to the fqrdd folder
	 * @params minLength: minimum sequence length wanted 
	 * @params maxLength: maximum sequence length wanted
	 * @params minQual: minimum sequence length wanted 
	 * @params maxQual: maximum sequence length wanted
	 * @params atgc: specifies if the sequence should only have atgc nucleotides
	 */
	public void filterFqRdd(JavaSparkContext sc, String folderPath,
			final int minLength,final int maxLength, final int minQual, final int maxQual, boolean atgc) throws IOException{
		JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc,folderPath);
		
		fqEditor.filterJavaRdd(fqrdd,minLength, maxLength, minQual, maxQual, atgc);
		
		
	}
	
	// with ID: il faudrait qu'une fonction qui gère with id or not (si c'est NULL or pas)
	public void filterFqRddwID(JavaSparkContext sc, String folderPath, String idFilePath,
			final int minLength,final int maxLength, final int minQual, final int maxQual, boolean atgc) throws IOException{
		//JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc,folderPath);
			
		File headerFile = new File(idFilePath);
		final List<String> headers = Files.readAllLines(FileSystems.getDefault().getPath(idFilePath));
		
		JavaRDD<FastqRecord>  fqrdd = readFqRDDFolder(sc,folderPath);
		JavaRDD<FastqRecord>  temp = fqEditor.filterJavaRdd(fqrdd, minLength, maxLength, minQual, maxQual, atgc);
		JavaRDD<FastqRecord>  fltrRes = temp.filter(new Function<FastqRecord,Boolean> (){
			public Boolean call (FastqRecord fq ){
				return headers.contains(fq.getSequenceHeader());
				
			}
		});
		
		
		// return fltrRes;
		
	}
	
	public void sampleFqRdd(JavaSparkContext sc, String folderPath, int nb, int seed) throws IOException
	{
		JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc,folderPath);
		fqEditor.sampleFqRdd(fqrdd, nb, seed); 
	}

	public void trimFqRdd (JavaSparkContext sc, String folderPath, final int minQuality, final int windowSize) 
			throws IOException
	{
		JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc,folderPath);
		fqEditor.trimFqRdd(fqrdd, minQuality, windowSize);
	}
	
	public void extractKmersFromFqRDD(JavaSparkContext sc, String folderPath, int nb, String outputPath) 
			throws IOException{
		KmeanMatrix.init(nb);		
		JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc,folderPath);
		fqEditor.countAllKmers(fqrdd);
		
		KmeanMatrix.getInstance().exportMatrix(outputPath);
		
	}
	
	public void getKmerKmeansClustering(JavaSparkContext sc, String folderPath, int order, int nbClusters, String outputPath)
			throws IOException
	{
		KmeanMatrix.init(order);		
		JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc,folderPath);
		fqEditor.countAllKmers(fqrdd);

		String filepath = KmeanMatrix.getInstance().exportDataToLIBSVM(outputPath);
		System.out.println("FilePAth:"+filepath);
		
		fqEditor.doKmeansClusterisation(filepath, nbClusters);		
		   
	}

}
