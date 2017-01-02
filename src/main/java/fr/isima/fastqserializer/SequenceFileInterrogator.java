package fr.isima.fastqserializer;

import htsjdk.samtools.fastq.FastqRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SequenceFileInterrogator {
	
	/*
	 * Prints out different statistiques about an fqrdd
	 * 
	 */
	public void getFqRDDSatistics(JavaSparkContext sc, String folderPath) throws IOException{
		System.out.println("=== Statistiques ===");
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
		
		long nbEntries ;
		
		nbEntries = fqrdd.count();
		System.out.println("Number of entries: "+nbEntries);
		
		/*
		 * Isolating Sequences
		 */
		JavaRDD<String> sequences = fqrdd.flatMap(new FlatMapFunction<FastqRecord, String>(){
			public Iterator<String> call(FastqRecord r) {
				List<String> list = Arrays.asList( r.getReadString().split(" "));
				Iterable<String> iter = list;
				return iter.iterator(); 
			}
 		});
		
		/*
		 * Isolating Nucléotides
		 */
		 JavaRDD<String> nucleotides = sequences.flatMap(new FlatMapFunction<String, String>(){
		 
			public Iterator<String> call(String s){
				List<String> list =  Arrays.asList(s.split(""));
				Iterable<String> iter = list;
				return iter.iterator();
			}
		});
		
		 /* 
		  * Count of the Nucleotides
		  */
		 //Map
		JavaPairRDD<String,Integer> pairs = nucleotides.mapToPair(new PairFunction<String, String, Integer>(){
			public Tuple2<String,Integer> call(String s){
				return new Tuple2<String,Integer>(s,1);
			}
		});
		
		//reduce
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			  public Integer call(Integer a, Integer b) { return a + b; }
		});
		/* ********************************* */
		
		
		/*
		 * Mean Quality per Sequences
		 */
		JavaPairRDD<String,Integer> meanSequencesQualities = fqrdd.mapToPair(new PairFunction<FastqRecord, String,Integer>(){
			public Tuple2<String, Integer> call(FastqRecord r) {
				return new Tuple2<String,Integer> (r.getReadString(), getSequenceQuality(r)/r.length()) ;  // Recupération des séquences
			}
		});
		/* *************************** */
		
		/*
		 * Getting Distributions
		 */
		// MAP
		JavaPairRDD<Integer,Integer> distributions  = sequences.mapToPair(new PairFunction<String, Integer , Integer>(){
			public Tuple2<Integer,Integer> call(String s){
				return new Tuple2<Integer,Integer>(s.length(),1);
			}
		});
		
		//REDUCE
		JavaPairRDD<Integer, Integer> distLenght = distributions.reduceByKey(new Function2<Integer, Integer, Integer>() {
			  public Integer call(Integer a, Integer b) { return a + b; }
		});
		
		/* ****************************************** */
		
		
		/*
		 * Getting each sequence and their quality
		 */
		//<Seq,Quality>
		JavaPairRDD<String,String> SeqQualities = fqrdd.mapToPair(new PairFunction<FastqRecord, String,String>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, String> call(FastqRecord r) {
				return new Tuple2<String,String> (r.getReadString(), r.getBaseQualityString());
			}
		});
		
		
		System.out.println("------------------------------");
		System.out.println("\t  ENTRY STATS");
		System.out.println("------------------------------");
		System.out.println("Sequences and Mean Quality");
		
		meanSequencesQualities.foreach(new VoidFunction<Tuple2<String, Integer>>(){
			public void call(Tuple2<String, Integer> t){
				//System.out.println(t._1 + " -> "+t._2);
			}
		} );
		
		System.out.println("------------------------------");
		System.out.println("Nucleotides found");
		counts.foreach(new VoidFunction<Tuple2<String, Integer>>(){
			public void call(Tuple2<String, Integer> t){
				System.out.println(t._1 + " -> "+t._2);
			}
		} );
		
		System.out.println("---3---------------------------");
		System.out.println("Sequences and length");
		sequences.foreach(new VoidFunction<String>(){
			public void call(String s){
			//	System.out.println("X"+ " -> " + s.length());
			}
		});
		
		System.out.println("------------------------------");
		System.out.println("Distributions found");
		distLenght.foreach(new VoidFunction<Tuple2<Integer, Integer>>(){
			public void call(Tuple2<Integer, Integer> t){
				System.out.println(t._1 + " -> "+t._2);
			}
		} );
		
		System.out.println("------------------------------");
		System.out.println("Mean Quality per position!");
		System.out.println("------------------------------");
		JavaRDD<FastqRecord> meanQual = fqrdd;
		int pos = 0;
		long nb = meanQual.count(); // on devrait utilisere nbEntries
		
		boolean cont = !meanQual.isEmpty();
		for(;cont == true ; ){
			
			JavaPairRDD<Integer,Integer>  res= meanQualityInPos(meanQual, pos);
			
			JavaPairRDD<Integer, Integer> quality = res.reduceByKey(new Function2<Integer, Integer, Integer>() {
				  public Integer call(Integer a, Integer b) { return a + b; }
			});
			
			System.out.println( quality.first()._1 + " -> "  + quality.first()._2 + " / " + nb); 
			System.out.println( quality.first()._1 + " -> "  + quality.first()._2 / nb); 
			
			//meanQual = filterJavaRdd(meanQual, pos, Integer.MAX_VALUE, 0, Integer.MAX_VALUE,true);
			nb = meanQual.count();
			++pos;
			cont = !meanQual.isEmpty();
			//cont = nb > pos;
			
		}
		
		/* ************************ */
		
		/*
		 * Fonction qui retourne la qualité moyenne pour une position parmis un ensemble de séquence
		 */
		private JavaPairRDD<Integer,Integer> meanQualityInPos(JavaRDD<FastqRecord> fqrdd, final int pos){
			JavaPairRDD<Integer,Integer> meanPositionQualities = fqrdd.mapToPair( new PairFunction<FastqRecord, Integer, Integer>(){
				public Tuple2<Integer,Integer> call(FastqRecord fq){
					
					return new Tuple2<Integer,Integer>(pos,getQualityValue(fq.getBaseQualityString().charAt(pos)));
				}
	 		});
			return meanPositionQualities;
		}
		
	}
}
