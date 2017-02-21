package fr.isima.fastqserializer;

import fr.isima.entities.KmeanMatrix;
import fr.isima.fastxrecord.*;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class SequenceFileEditor implements Serializable {

	/*
	 * Filters an JavaRdd of FqRecords according to criterias specified
	 * @params fqrdd: JavaRDD to filter
	 * @params minLength: minimum sequence length wanted 
	 * @params maxLength: maximum sequence length wanted
	 * @params minQual: minimum sequence length wanted 
	 * @params maxQual: maximum sequence length wanted
	 * @params atgc: specifies if the sequence should only have atgc nucleotides
	 * @return : the filtered version of the JavaRDD 
	 */
	public JavaRDD<FastqRecord> filterJavaRdd(JavaRDD<FastqRecord> fqrdd,
			final int minLength,final int maxLength, final int minQual, final int maxQual, boolean atgc) throws IOException{
		
		JavaRDD<FastqRecord> fltrRes = fqrdd.filter(new Function<FastqRecord,Boolean> (){
			public Boolean call (FastqRecord fq ){
				int quality = fq.getMeanQuality();
				int len = fq.getLength();
				return (len >= minLength)
						&& (len <= maxLength)
						&& (quality >= minQual)
						&& (quality <= maxQual);  
			}
		});
		
		if(atgc){
			fltrRes = fltrRes.filter(new Function<FastqRecord, Boolean>(){
				public Boolean call (FastqRecord fq){
					//return !fq.getReadString().contains("N"); // retourne que si il y a atgc	
					String seqLeft = fq.getSequenceString().replace("A", "")
							.replace("T", "")
							.replace("G", "")
							.replace("C", "");
					boolean res = !(seqLeft
							.length() > 0); //TODO Test
					
					//System.out.println("SEQ LEFT!! -- ." + seqLeft +'.');
					return res;
				}
			});
		}
		return fltrRes;
	}
	
	/*
	 * Returns a random arrangement of sequences in an FqRdd
	 * @params fqrdd: RDD to sample
	 * @params nb: number of sequences wanted
	 * @params seed: seed for the shuffle
	 * @return the sample 
	 */
	public JavaRDD<FastqRecord> sampleFqRdd(JavaRDD<FastqRecord> fqrdd, int nb, int seed) throws IOException
	{
		double percent  = nb / fqrdd.count();
		JavaRDD<FastqRecord> fqresult = fqrdd.sample(false, percent, seed);
		
		 return fqresult;
	
	}
	
	/*
	 * Trims a Sequences according to the criterias given
	 * @params fqrdd: RDD to trim
	 * @params minQuality: minimum quality each sequence should have at the end
	 * @parmas windowSize: windowSize of the Quality Search
	 * @return : returns the trimmed sequences
	 * 
	 */
	public JavaRDD<FastqRecord> trimFqRdd(JavaRDD<FastqRecord> fqrdd, final int minQuality, final int windowSize) 
			throws IOException
	{
		
		// filtrer sur la qualité moyenne et la longueur de la séquence

		JavaRDD<FastqRecord> fltrRes = fqrdd.filter(new Function<FastqRecord,Boolean> (){
			public Boolean call (FastqRecord fq ){
				int quality = fq.getMeanQuality();
				return quality >= minQuality && fq.getLength() >= 100; //TODO  Verfifier avec Kévin
			}
		});
		
		fltrRes.foreach(new VoidFunction<FastqRecord>(){
			public void call(FastqRecord fq){
				int length = fq.getLength();
		
				int idBegin = 0;
				int idEnd = length -1;
				while(idBegin < length - 1 &&
						fq.getEncoding().getQualityValue(fq.getQualityString().charAt(idBegin)) < minQuality){
					//TODO  Verfifier avec Kévin
					++idBegin;
				}
				//TODO  Verifier avec Kévin
				if(length - idBegin > 60){ 
					
					idEnd = idBegin;
					while( length - idEnd -1 > windowSize && 
							fq.getEncoding().getQualityValue(fq.getQualityString().substring(idEnd, idEnd + windowSize))
									> minQuality)
							
					{
						++idEnd;
					}
					idEnd += windowSize;
					
				}
				// VERIFIER SI L'OBJET PASSE EN ENTREE A ETE MODIFIE
				fq = new FastqRecord(fq.getSequenceHeader(), fq.getSequenceString().substring(idBegin, idEnd), 
						fq.getQualityHeader(), 
						fq.getQualityString().substring(idBegin, idEnd));
				
				//FastqRecord test = new FastqRecord(seqHeaderPrefix, seqLine, qualHeaderPrefix, qualLine)
			
				
			}
		});
		
		// filtrage pour voir si les séquence obtenu du trim sont interessantes ou pas
		JavaRDD<FastqRecord> last = fltrRes.filter(new Function<FastqRecord,Boolean> (){
			public Boolean call (FastqRecord fq ){
				
				return fq.getLength() >= 100 ; //TODO  Verfifier avec Kévin
			}
		});
		
		return last;
	}
	
	public void countAllKmers(JavaRDD<FastqRecord> fq){
		
		fq.foreach(new VoidFunction<FastqRecord>(){
			public void call(FastqRecord fq){
				//System.out.println("-- FqAdding: "+ fq.getSequenceHeader());
				String sub = fq.getSequenceString();
				int k = KmeanMatrix.getKeyLength();
				int i = 0;
				while(sub.length() - i >= k){
					String key =sub.substring(i, i+k);
					KmeanMatrix.getInstance().add(key, fq.getSequenceHeader());
					i++;
				}
				
				
			}
		});
		
	}
	
}
