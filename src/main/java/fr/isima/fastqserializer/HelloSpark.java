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
		String filename = "SP1";
		String filePath = "./data/"+filename+".fq";
		String folderPath = "./results/" + filename +".fqrdd";
		String resultPath = "./results/" + filename +"output";
		SequenceFileManager fqman = new SequenceFileManager();
		
		try {
			//fqman.readFastqFile(filePath);
			//fqman.convertFastqToFqrdd(sc,filePath,folderPath);
			//fqman.getFqRDDSatistics(sc,folderPath);
			fqman.getKmerKmeansClustering(sc, folderPath, 4, 2,resultPath);
			//fqman.trimFqRdd(sc, folderPath, 15000, 5);
			
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		
		sc.close();
		System.out.println("Goodbye.");
		
	}

	
}
