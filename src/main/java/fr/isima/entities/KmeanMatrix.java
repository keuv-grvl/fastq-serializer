package fr.isima.entities;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import com.google.gson.Gson;

public class KmeanMatrix implements Serializable {
	
	private static int complexity = 0;
	
	/** map <idLecture, map<Kmer, nbOcc>> **/
	private ConcurrentHashMap<String,
	ConcurrentHashMap<String,Integer>> map ;
	
	/** Features: ensemble des kmer que l'on recherche **/
	ConcurrentHashMap<String,Integer> features;
	
	/** Constructeur privé */	
	private KmeanMatrix(int k)
	{
		map = new ConcurrentHashMap<String, ConcurrentHashMap<String,Integer>>();
		features  = new ConcurrentHashMap<String, Integer>();
		/** première version du singleton: instancier la matrice avec tous les k-mers 
		 * et ensuite rajouter les id de lecture au fur et à mesure de la lecture
		 * map <Kmeans, map<idLecture, nbOcc>>
		 * afin de pouvoir avoir ts les kmer possible et éviter un if sur la partie d'ajout
		 */
		
		// Create an initial vector of 5 elements (A, T,G,C,N)
		   ICombinatoricsVector<String> originalVector = Factory.createVector(new String[] { "A","T","G","C","N" });
		   Generator<String> gen = Factory.createPermutationWithRepetitionGenerator(originalVector, k);
		   
		   for (ICombinatoricsVector<String> perm : gen){
			   String key = "";
			   for(int i = 0; i < perm.getSize(); i++)
				   key+= perm.getValue(i);
			   
			   System.out.println(key);
			   features.put(key,0);
		   }
		
	}
 
	/** Holder */
	private static class SingletonHolder
	{		
		/** Instance unique non préinitialisée */
		private final static KmeanMatrix instance = new KmeanMatrix(complexity);
	}
 
	/** Point d'accès pour l'instance unique du singleton */
	public static KmeanMatrix getInstance()
	{
		return SingletonHolder.instance;

	}
	
	/** afin d'initialiser notre matrice avec un nombre de k-mer **/
	public static KmeanMatrix init(int k){
		if(complexity != 0){
			//System.out.println("INSTANCES = 1");
			return null;
		}
		else{
			//System.out.println("INSTANCES = 0");
			complexity = k;
			return getInstance();
		}
	}
	
	/** Sécurité anti-désérialisation */
	private Object readResolve() {
		return SingletonHolder.instance;
	}
	
	public static int getComplexity(){
		return complexity;
	}
	
	public void addSequence(String seqHeader){
		if(!map.containsKey(seqHeader)){
			
			map.put(seqHeader, new ConcurrentHashMap<String, Integer>(features));
		}
	}
	
	public void add(String seq, String kmer){
		if(map.containsKey(seq)){
			int val = map.get(seq).get(kmer);
			map.get(seq).put(kmer , val+1);
		}
	
	}
	
	public void print(){
		//Gson gson = new Gson();
		//System.out.println(gson.toJson(map));
		//*
		for(String s : map.keySet()){
			System.out.println(s);
			for(String st : map.get(s).keySet()){
				System.out.println("\t"+ st + ": "+ map.get(s).get(st));
			}
		}
		//*/
	}
	
	public String toJson(){
		Gson gson = new Gson();
		return gson.toJson(map);
	}
	
	public void exportMatrix(String folderpath){
		List<String> outputFiles = new ArrayList<String>();
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd.HHmmssSSS");
		Date date = new Date();
		
		String filename = "/KmeansMatrix_" + dateFormat.format(date);
		outputFiles.add(exportAllMatrixToCSV(folderpath, filename));
		outputFiles.add(exportDataToLIBSVM(folderpath, filename));
		
		System.out.println("Exported Files: ");
		for(String str: outputFiles){
			System.out.println("\t -- " + str);
		}
			
	}
	
	public String exportAllMatrixToCSV(String folderpath){
		
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd.HHmmssSSS");
		Date date = new Date();
		
		return exportAllMatrixToCSV(folderpath,"/KmeansMatrix_" + dateFormat.format(date));
	}
	
	public String exportAllMatrixToCSV(String folderpath,String outputName){		
		
		String filename = folderpath + outputName +".csv";
		Path file = Paths.get(filename);
		
		List<String> lines = new ArrayList<String>();
		
		// header
		String header = "Sequences;";
		for(String feat: features.keySet())
			header+= feat+";";
		lines.add(header);
		
		for(String seq : map.keySet()){
			String newLine = seq+";";
			for(String feat:features.keySet()){
				newLine += map.get(seq).get(feat) +";";
			}
			lines.add(newLine);
		}
			
		try {
			Files.write(file, lines, Charset.forName("UTF-8"));
		} catch (IOException e) {
			System.err.println("ERREUR DANS L'EXPORTATION DE LA MATRICE EN CSV");
			e.printStackTrace();
		}
		return filename;
	}
	public String exportDataToLIBSVM(String folderpath){
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd.HHmmssSSS");
		Date date = new Date();
		
		return exportDataToLIBSVM(folderpath,"/KmeansMatrix_" + dateFormat.format(date));
	}
	
	public String exportDataToLIBSVM(String folderpath, String outputName){

		
		String filename = folderpath + outputName +".text";
		Path file = Paths.get(filename);
		
		List<String> lines = new ArrayList<String>();
		int i = 0;

		for(String seq : map.keySet()){
			int j = 1;
			String newLine = i+" ";
			for(String feat:features.keySet()){
				newLine += j+":"+map.get(seq).get(feat) +" ";
				j++;
			}
			lines.add(newLine);
			i++;
		}
			
		try {
			Files.write(file, lines, Charset.forName("UTF-8"));
		} catch (IOException e) {
			System.err.println("ERREUR DANS L'EXPORTATION DE LA MATRICE EN LIBSVM");
			e.printStackTrace();
		}
		return filename;
	}
	
	
	
}
