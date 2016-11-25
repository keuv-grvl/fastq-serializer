package fr.isima.fastqserializer;

import htsjdk.samtools.fastq.FastqRecord;

import java.io.IOException;
import java.util.Arrays;

import org.apache.spark.api.java.*;
import org.apache.spark.*;

public class HelloSpark {

	public static void main(String[] args) {
		System.out.println("Hello World!");
		
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
				.set("spark.driver.maxResultSize", "0")
				.set("spark.rdd.compress", "true")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses((Class<?>[]) Arrays.asList(FastqRecord.class).toArray());  
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		sc.setLogLevel("OFF"); //ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
		
		System.out.println(sc.appName());
		
		
		
		//System.out.println("Goodbye.");
		String filePath = "./data/SP2.fq";
		String folderPath = "./results/SP2.fqrdd";
		String resultPath = "./results/SP2.fqrdd";
		FastqFileManager fqman = new FastqFileManager();
		
		try {
			//fqman.readFastqFile(filePath);
			//fqman.convertFastqToFqrdd(sc,filePath,resultPath);
			fqman.getFqRDDSatistics(sc,folderPath);
			//fqman.readFqRDD(sc,"./results/SP1.fqrdd/part-00000");
			
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		
		sc.close();
		System.out.println("Goodbye.");
		
	}
}
