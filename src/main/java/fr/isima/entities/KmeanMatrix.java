package fr.isima.entities;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import com.google.gson.Gson;

public class KmeanMatrix implements Serializable {
	
	private static int complexity = 0;
	
	/** map <Kmeans, map<idLecture, nbOcc>> **/
	private ConcurrentHashMap<String,
	ConcurrentHashMap<String,Integer>> map ;
	
	/** Features: ensemble des kmer que l'on recherche **/
	ConcurrentHashMap<String,Integer> features;
	
	/** Constructeur privé */	
	private KmeanMatrix(int k)
	{
		map = new ConcurrentHashMap<String, ConcurrentHashMap<String,Integer>>();
		/** première version du singleton: instancier la matrice avec tous les k-mers 
		 * et ensuite rajouter les id de lecture au fur et à mesure de la lecture
		 * map <Kmeans, map<idLecture, nbOcc>>
		 * afin de pouvoir avoir ts les kmer possible et éviter un if sur la partie d'ajout
		 */
		
		// Create an initial vector of 5 elements (A, T,G,C,N)
		   ICombinatoricsVector<String> originalVector = Factory.createVector(new String[] { "A","T","G","C","N" });
		   Generator<String> gen = Factory.createPermutationWithRepetitionGenerator(originalVector, k);
		   
		   for (ICombinatoricsVector<String> perm : gen){
			   //System.out.println(perm);
			   String key = "";
			   for(int i = 0; i < perm.getSize(); i++)
				   key+= perm.getValue(i);
			   
			   //System.out.println(key);
			   map.put(key,new ConcurrentHashMap<String, Integer>());
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
	
	public static int getKeyLength(){
		return complexity;
	}
	
	public void add(String kmer, String seq){
		int val = 0;
		
		if(map.get(kmer).containsKey(seq)){
			val = map.get(kmer).get(seq);
		}
		
		map.get(kmer).put(seq , val+1);
	
	}
	
	public void print(){
		Gson gson = new Gson();
		System.out.println(gson.toJson(map));
		/*
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
	
	
	
}
