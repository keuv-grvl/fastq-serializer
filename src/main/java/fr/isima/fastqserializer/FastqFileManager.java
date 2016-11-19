package fr.isima.fastqserializer;

import htsjdk.samtools.fastq.*;
//import uk.ac.babraham.FastQC.Sequence.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;


public class FastqFileManager implements Serializable{
	
	private int getSequenceQuality(FastqRecord fqrecord){
		int res = 0;
		
		String[] nucleos = fqrecord.getReadString().split("");
		String[] qualities = fqrecord.getBaseQualityString().split("");
		
		for(int i = 0; i < fqrecord.length(); i++){
			res += getQualityValue(qualities[i].charAt(0));
		}
		return res;
	}
	
	private int getQualityValue(char qualityChar){
		return (int) qualityChar - (int) '!';
	}
	
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
                	 System.out.println(str);
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

	public void readFastqFile(String filePath) throws IOException {
		FastqReader fastqReader = new FastqReader(new File(filePath));

		for (FastqRecord fastq : fastqReader)
		{
			System.out.println("Sequence: " + fastq.toString());
			System.out.println("\t getBaseQualityHeader: " + fastq.getBaseQualityHeader());
			System.out.println("\t getBaseQualityString: " + fastq.getBaseQualityString());
			System.out.println("\t getReadHeader: " + fastq.getReadHeader());
			System.out.println("\t getReadString: " + fastq.getReadString());
			System.out.println("\t  	hashCode: " + fastq.hashCode());
			System.out.println("\t  	length: " + fastq.length());
			System.out.println("------------------------------- ");

		}
		fastqReader.close();
	}

	public void convertFastqToFqrdd(JavaSparkContext sc, String filePath) throws IOException {
		
		List<FastqRecord> fastqArray = getFqArray(filePath);
		
		/* Transformation des objets fastq en objets RDD */
		System.out.println("Parallelisation");
		JavaRDD<FastqRecord> fastqRDD =  sc.parallelize(fastqArray);
		
		

		System.out.println("Exportation");
		// si le dossier existe déjà, il lance une erreur
		fastqRDD.saveAsObjectFile("./results/SP1.fqrdd");
		//fastqRDD.saveAsObjectFile("./results/temp/SP1.fqrdd");
	}
	
	public void getFqRDDSatistics(JavaSparkContext sc, String folderPath) throws IOException{
		System.out.println("Début");
		//List<FastqRecord> fastqArray = getFqArray(filePath);
		JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc,folderPath);
		/* We want to know:

		    number of:
		        entries
		        nucleotides (A, T, G, C, N)
		    sequence length:
		        for each entry
		        distribution
		    mean quality:
		        per entry
		        per nucleotide position for all entries
		        distribution
		 */
		
		/* Il faut faire des map reduce 
		 * utiliser la méthode filter pour récupérer toutes les séquences 
		 * 		et de ces séquences compter 5 fois la lettre qui nous interesse ds le cas présent. 
		 */
		
		long nbEntries ;
		
		nbEntries = fqrdd.count();
		System.out.println("Number of entries: "+nbEntries);
		
		JavaRDD<String> sequences = fqrdd.flatMap(new FlatMapFunction<FastqRecord, String>(){
			public Iterator<String> call(FastqRecord r) {
				List<String> list = Arrays.asList( r.getReadString().split(" "));
				Iterable<String> iter = list;
				return iter.iterator();   // Recupération des séquences
			}
		});
		
		 JavaRDD<String> nucleotides = sequences.flatMap(new FlatMapFunction<String, String>(){
		 
			public Iterator<String> call(String s){
				List<String> list =  Arrays.asList(s.split(""));
				Iterable<String> iter = list;
				return iter.iterator();
			}
		});
		
		JavaPairRDD<String,Integer> pairs = nucleotides.mapToPair(new PairFunction<String, String, Integer>(){
			public Tuple2<String,Integer> call(String s){
				return new Tuple2<String,Integer>(s,1);
			}
		});
		
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			  public Integer call(Integer a, Integer b) { return a + b; }
		});
		
		JavaPairRDD<String,Integer> meanSequencesQualities = fqrdd.mapToPair(new PairFunction<FastqRecord, String,Integer>(){
			public Tuple2<String, Integer> call(FastqRecord r) {
				return new Tuple2<String,Integer> (r.getReadString(), getSequenceQuality(r)/r.length()) ;  // Recupération des séquences
			}
		});
		/* *************************** */
		//<Seq,Quality>
		JavaPairRDD<String,String> SeqQualities = fqrdd.mapToPair(new PairFunction<FastqRecord, String,String>(){
			public Tuple2<String, String> call(FastqRecord r) {
				return new Tuple2<String,String> (r.getReadString(), r.getBaseQualityString());
			}
		});
		
		// TODO
		JavaPairRDD<Integer,Integer> meanPositionQualities = fqrdd.mapToPair(new PairFunction<FastqRecord, Integer,Integer>(){
			public Tuple2<Integer, Integer> call(FastqRecord r) {
				return new Tuple2<Integer,Integer> (r.getReadString().length(), getSequenceQuality(r)/r.length()) ;  // Recupération des séquences
			}
		});
		//TODO  terminer de determiner le mean quality per position
		/* ************************ */
		System.out.println("------------------------------");
		System.out.println("\t  ENTRY STATS");
		System.out.println("------------------------------");
		System.out.println("Sequences and Mean Quality");
		meanSequencesQualities.foreach(new VoidFunction<Tuple2<String, Integer>>(){
			public void call(Tuple2<String, Integer> t){
				System.out.println(t._1 + " -> "+t._2);
			}
		} );
		
		System.out.println("------------------------------");
		System.out.println("Nucleotides found");
		counts.foreach(new VoidFunction<Tuple2<String, Integer>>(){
			public void call(Tuple2<String, Integer> t){
				System.out.println(t._1 + " -> "+t._2);
			}
		} );
		
		System.out.println("------------------------------");
		System.out.println("Sequences and length");
		sequences.foreach(new VoidFunction<String>(){
			public void call(String s){
				System.out.println(s+ " : " + s.length());
			}
		});
		
		
	}
	
	public void filterFqRdd(JavaSparkContext sc, String filepath) throws IOException{
		List<FastqRecord> fastqArray = getFqArray(filepath);
		JavaRDD<FastqRecord> fqrdd = sc.parallelize(fastqArray);
		
		// EXEMPLE: Filtre par rapport à la longueur de la séquence par exemple
		JavaRDD<FastqRecord> fltrRes = fqrdd.filter(new Function<FastqRecord,Boolean> (){
			public Boolean call (FastqRecord fq ){
				return fq.getReadString().length() > 10;  
			}
		});
		
	}
	
	
	
}
