package fr.isima.fastqserializer;

import htsjdk.samtools.fastq.FastqReader;
import htsjdk.samtools.fastq.FastqRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class SequenceFileEditor {

	/*
	 * Filters a JAVARDD according to the criterias given
	 */
	public JavaRDD<FastqRecord> filterJavaRdd(JavaRDD<FastqRecord> fqrdd,
			final int minLength,final int maxLength, final int minQual, final int maxQual, boolean atgc) throws IOException{
		
		JavaRDD<FastqRecord> fltrRes = fqrdd.filter(new Function<FastqRecord,Boolean> (){
			public Boolean call (FastqRecord fq ){
				int quality = getSequenceQuality(fq);
				int len = fq.length();
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
					String seqLeft = fq.getReadString().replace("A", "")
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
	
	public void sampleFqRdd(JavaSparkContext sc, String folderPath, int nb, int seed) throws IOException
	{
		JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc,folderPath);
		
		double percent  = nb / fqrdd.count();
		JavaRDD<FastqRecord> fqresult = fqrdd.sample(false, percent, seed);
		
		// return fqresult;
	
	}
	
	public void trimFqRdd (JavaSparkContext sc, String folderPath, final int minQuality, final int windowSize) throws IOException{
		JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc,folderPath);
		
		// filtrer sur la qualité moyenne et la longueur de la séquence

		JavaRDD<FastqRecord> fltrRes = fqrdd.filter(new Function<FastqRecord,Boolean> (){
			public Boolean call (FastqRecord fq ){
				int quality = getSequenceQuality(fq);
				return quality >= minQuality && fq.length() >= 100; //TODO  Verfifier avec Kévin
			}
		});
		
		fltrRes.foreach(new VoidFunction<FastqRecord>(){
			public void call(FastqRecord fq){
				int length = fq.length();
		
				int idBegin = 0;
				int idEnd = length -1;
				while(idBegin < length - 1 &&
						getQualityValue( fq.getBaseQualityString().charAt(idBegin)) < minQuality){//TODO  Verfifier avec Kévin
					++idBegin;
				}
				//TODO  Verifier avec Kévin
				if(length - idBegin > 60){ 
					
					idEnd = idBegin;
					while( length - idEnd -1 > windowSize && 
							getStringQuality(fq.getBaseQualityString().substring(idEnd, idEnd + windowSize))
									> minQuality)
					{
						++idEnd;
					}
					idEnd += windowSize;
					
				}
				// VERIFIER SI L'OBJET PASSE EN ENTREE A ETE MODIFIE
				fq = new FastqRecord(fq.getReadHeader(), fq.getReadString().substring(idBegin, idEnd), 
						fq.getBaseQualityHeader(), 
						fq.getBaseQualityString().substring(idBegin, idEnd));
				
				//FastqRecord test = new FastqRecord(seqHeaderPrefix, seqLine, qualHeaderPrefix, qualLine)
			
				
			}
		});
		
		// filtrage pour voir si les séquence obtenu du trim sont interessantes ou pas
		JavaRDD<FastqRecord> last = fltrRes.filter(new Function<FastqRecord,Boolean> (){
			public Boolean call (FastqRecord fq ){
				
				return fq.length() >= 100 ; //TODO  Verfifier avec Kévin
			}
		});
	}
}
