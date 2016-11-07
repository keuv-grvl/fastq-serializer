package fr.isima.fastqserializer;

import org.apache.spark.api.java.*;
import org.apache.spark.*;

public class HelloSpark {

	public static void main(String[] args) {
		System.out.println("Hello World!");

		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		sc.setLogLevel("OFF"); //ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
		
		System.out.println(sc.appName());
		
		sc.close();
		
		System.out.println("Goodbye.");
	}
}
