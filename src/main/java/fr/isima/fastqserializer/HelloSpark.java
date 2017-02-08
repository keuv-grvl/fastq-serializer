package fr.isima.fastqserializer;

import fr.isima.fastxrecord.*;

import java.io.IOException;
import java.util.Arrays;

import org.apache.spark.api.java.*;
import org.apache.spark.*;
public class HelloSpark {
	public static void main(String[] args) {
		System.out.println("Hello World!");
		
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[5]")
				.set("spark.driver.maxResultSize", "0")
				.set("spark.rdd.compress", "true")
				//.set("mapreduce.output.fileoutputformat.compress", "true")
				//.set("spark.io.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec") //org.apache.hadoop.io.compress.BZip2Codec
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses((Class<?>[]) Arrays.asList(FastqRecord.class).toArray());  
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		sc.setLogLevel("OFF"); //ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
		
		System.out.println(sc.appName());		
		
		//System.out.println("Goodbye.");
		String filePath = "./data/SP1.fq";
		String folderPath = "./results/SP1.fqrdd";
		String resultPath = "./results/SP1.fqrdd";
		SequenceFileManager fqman = new SequenceFileManager();
		
		try {
			//fqman.readFastqFile(filePath);
			//fqman.convertFastqToFqrdd(sc,filePath,resultPath);
			//fqman.getFqRDDSatistics(sc,folderPath);
			fqman.getAllKMers(sc, folderPath, 4, resultPath);
			//fqman.readFqRDD(sc,"./results/SP1.fqrdd/part-00000");
			//fqman.trimFqRdd(sc, folderPath, 15000, 5);
			
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		
		sc.close();
		System.out.println("Goodbye.");
		
	}

	
}

// Old main
/*
public static void main(String[] args) {
	System.out.println("Hello World!");
	
	SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[20]")
			.set("spark.driver.maxResultSize", "0")
			.set("spark.rdd.compress", "true")
			//.set("mapreduce.output.fileoutputformat.compress", "true")
			//.set("spark.io.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec") //org.apache.hadoop.io.compress.BZip2Codec
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.registerKryoClasses((Class<?>[]) Arrays.asList(FastqRecord.class).toArray());  
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	sc.setLogLevel("OFF"); //ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
	
	System.out.println(sc.appName());
	
	
	
	//System.out.println("Goodbye.");
	String filePath = "./data/SP1.fq";
	String folderPath = "./results/SP1.fqrdd";
	String resultPath = "./results/SPTest.fqrdd";
	FastqFileManager fqman = new FastqFileManager();
	
	try {
		//fqman.readFastqFile(filePath);
		//fqman.convertFastqToFqrdd(sc,filePath,resultPath);
		fqman.getFqRDDSatistics(sc,folderPath);
		//fqman.readFqRDD(sc,"./results/SP1.fqrdd/part-00000");
		//fqman.trimFqRdd(sc, folderPath, 15000, 5);
		
	} 
	catch (IOException e) {
		e.printStackTrace();
	}
	
	sc.close();
	System.out.println("Goodbye.");
	
}
//*/
