package fr.isima.fastqserializer;

// changement au niveau de SequecesMeanQuality (chagement deString à FqRDD  (voir aussi l'affichage)
// ajout de la fonction getMaxNucleotideQuality getMin NucleotideQuality get Q1 et Q3
// ajout de "fill position"
import htsjdk.samtools.fastq.*;
//import uk.ac.babraham.FastQC.Sequence.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileSystems;
import java.nio.file.Files;
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

//import org.apache.hadoop.io.compress.bzip2.*;
//import org.apache.hadoop.io.compress.BZip2Codec;

import scala.Tuple2;

public class FastqFileManager implements Serializable {
	int positions[];

	private void addToArray(int[] positions, int numberOfPos, int numberOfTimes) {
		// System.out.println(positions[0]);

		for (int i = 0; i < numberOfPos; ++i) {
			positions[i] += numberOfTimes;
			// System.out.println("Adding: " + numberOfTimes + " to: " + i + " =
			// " + positions[i].intValue());
		}
	}

	private int getMaxNucleotideQuality(FastqRecord fqrecord) {

		int max = getQualityValue(fqrecord.getBaseQualityString().charAt(0));
		for (int i = 1; i < fqrecord.getBaseQualityString().length(); i++) {
			int temp = getQualityValue(fqrecord.getBaseQualityString().charAt(i));
			max = (temp > max) ? temp : max;
		}

		return max;
	}

	private int getMinNucleotideQuality(FastqRecord fqrecord) {

		int min = getQualityValue(fqrecord.getBaseQualityString().charAt(0));
		for (int i = 1; i < fqrecord.getBaseQualityString().length(); i++) {
			int temp = getQualityValue(fqrecord.getBaseQualityString().charAt(i));
			min = (temp < min) ? temp : min;
		}

		return min;
	}

	private int getQ1(FastqRecord fqrecord) {

		int quality = getSequenceQuality(fqrecord);
		int cumQual = 0;
		int dest = quality / 4;
		int i = 0;

		for (; cumQual < dest; i++) {
			cumQual += getQualityValue(fqrecord.getBaseQualityString().charAt(i));
		}

		return i;
	}

	private int getQ3(FastqRecord fqrecord) {

		int quality = getSequenceQuality(fqrecord);
		int cumQual = 0;
		int dest = quality * 3 / 4;
		int i = 0;

		for (; cumQual < dest; i++) {
			cumQual += getQualityValue(fqrecord.getBaseQualityString().charAt(i));
		}

		return i;
	}

	private int getSequenceQuality(FastqRecord fqrecord) {

		return getStringQuality(fqrecord.getBaseQualityString());
	}

	private int getStringQuality(String qualityString) {
		int res = 0;
		for (int i = 0; i < qualityString.length(); i++) {
			res += getQualityValue(qualityString.charAt(i));
		}
		return res;
	}

	private int getQualityValue(char qualityChar) {
		return qualityChar - '!';
	}

	/**
	 * Reads a file containing the sequences' data and constructs a List
	 * containing objects representing that Data
	 * 
	 * @param filepath
	 *            : path to the file
	 * @return list containing object representing the sequence data
	 */
	private List<FastqRecord> getFqArray(String filePath) {
		FastqReader fastqReader = new FastqReader(new File(filePath));
		List<FastqRecord> fastqArray = new ArrayList<FastqRecord>();

		for (FastqRecord fastq : fastqReader) {
			fastqArray.add(fastq);

		}
		fastqReader.close();
		return fastqArray;
	}

	private JavaRDD<FastqRecord> readFqRDDFolder(JavaSparkContext sc, String folderPath) throws IOException {
		JavaRDD<FastqRecord> fqrdd = sc.emptyRDD();
		File folder = new File(folderPath);
		File[] listOfFiles; // = folder.listFiles();

		System.out.println(folderPath);
		// create new filename filter
		FilenameFilter fileNameFilter = new FilenameFilter() {

			public boolean accept(File dir, String name) {
				if (name.lastIndexOf('-') > 0) {
					// get last index for '-' char

					int lastIndex = name.lastIndexOf('-');

					// get extension
					String str = name.substring(0, lastIndex);

					// match path name extension
					if (str.equals("part")) {
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
		for (File path : listOfFiles) {
			System.out.println(path.toString());
			JavaRDD<FastqRecord> temp = sc.objectFile(path.toString());
			fqrdd = temp.union(fqrdd);
		}
		return fqrdd;

	}

	public void readFastqFile(String filePath) throws IOException {
		FastqReader fastqReader = new FastqReader(new File(filePath));

		for (FastqRecord fastq : fastqReader) {
			System.out.println("Sequence: " + fastq.toString());
			System.out.println("\t getBaseQualityHeader: " + fastq.getBaseQualityHeader());
			System.out.println("\t getBaseQualityString: " + fastq.getBaseQualityString());
			System.out.println("\t  	value: " + getStringQuality(fastq.getBaseQualityString()));
			System.out.println("\t getReadHeader: " + fastq.getReadHeader());
			System.out.println("\t getReadString: " + fastq.getReadString());
			System.out.println("\t  	hashCode: " + fastq.hashCode());
			System.out.println("\t  	length: " + fastq.length());
			System.out.println("------------------------------- ");

		}
		fastqReader.close();
	}

	public void convertFastqToFqrdd(JavaSparkContext sc, String filePath, String resultFolder) throws IOException {

		List<FastqRecord> fastqArray = getFqArray(filePath);

		/* Transformation des objets fastq en objets RDD */
		System.out.println("Parallelisation");
		JavaRDD<FastqRecord> fastqRDD = sc.parallelize(fastqArray);

		System.out.println("Exportation");
		// si le dossier existe déjà, il lance une erreur
		fastqRDD.saveAsObjectFile(resultFolder);

		// using bzip2 to compress the output
		/*
		 * OutputStream fout = new org.apache.hadoop.io.compress.BZip2Codec()
		 * .createOutputStream(new FileOutputStream(resultFolder) );
		 * 
		 * Il faut ensuite écrire les bits...ou écire les objets
		 * https://www.mkyong.com/java/how-to-compress-serialized-object-into-
		 * file/
		 */
	}

	public void getFqRDDSatistics(JavaSparkContext sc, String folderPath) throws IOException {
		System.out.println("Début");
		final JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc, folderPath);

		// TOTEST

		// JavaRDD<FastqRecord> fqrdd =filterFqRdd(sc, folderPath, 0,
		// Integer.MAX_VALUE, 0, Integer.MAX_VALUE,true);
		/*
		 * We want to know:
		 * 
		 * number of: entries nucleotides (A, T, G, C, N) sequence length: for
		 * each entry distribution mean quality: per entry per nucleotide
		 * position for all entries distribution
		 */

		/*
		 * Il faut faire des map reduce utiliser la méthode filter pour
		 * récupérer toutes les séquences et de ces séquences compter 5 fois la
		 * lettre qui nous interesse ds le cas présent.
		 */

		long nbEntries;

		nbEntries = fqrdd.count();
		System.out.println("Number of entries: " + nbEntries);

		/*
		 * Récupération des séquences lues
		 */
		JavaRDD<String> sequences = fqrdd.flatMap(new FlatMapFunction<FastqRecord, String>() {
			public Iterator<String> call(FastqRecord r) {
				List<String> list = Arrays.asList(r.getReadString().split(" "));
				Iterable<String> iter = list;
				return iter.iterator();
			}
		});

		/*
		 * Isolation de chaque Nucléotides dans toutes les séquences
		 */
		JavaRDD<String> nucleotides = sequences.flatMap(new FlatMapFunction<String, String>() {

			public Iterator<String> call(String s) {
				List<String> list = Arrays.asList(s.split(""));
				Iterable<String> iter = list;
				return iter.iterator();
			}
		});

		/*
		 * Compte de chaque nucléotides
		 */
		// Map
		JavaPairRDD<String, Integer> pairs = nucleotides.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		// reduce
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});
		/* ********************************* */

		/*
		 * Récupération de la qualité moyenne de chaque séquences
		 */
		JavaPairRDD<FastqRecord, Integer> meanSequencesQualities = fqrdd
				.mapToPair(new PairFunction<FastqRecord, FastqRecord, Integer>() {
					public Tuple2<FastqRecord, Integer> call(FastqRecord r) {
						return new Tuple2<FastqRecord, Integer>(r, getSequenceQuality(r) / r.length()); // Recupération
																										// des
																										// séquences
					}
				});

		/*
		 * Qualité moyenne totale
		 */
		JavaPairRDD<Integer, Integer> allTotalSequencesQualities = fqrdd
				.mapToPair(new PairFunction<FastqRecord, Integer, Integer>() {
					public Tuple2<Integer, Integer> call(FastqRecord r) {
						return new Tuple2<Integer, Integer>(r.length(), getSequenceQuality(r)); // Recupération
																								// des
																								// séquences
					}
				});

		JavaRDD<Integer> justKeys = allTotalSequencesQualities.keys();
		int totalNucleotides = justKeys.reduce(new Function2<Integer, Integer, Integer>() {
			// Reduce Function for cumulative sum
			public Integer call(Integer accum, Integer val) throws Exception {
				return accum + val;
			}
		});

		JavaRDD<Integer> justValues = allTotalSequencesQualities.values();
		int totalQuality = justValues.reduce(new Function2<Integer, Integer, Integer>() {
			// Reduce Function for cumulative sum
			public Integer call(Integer accum, Integer val) throws Exception {
				return accum + val;
			}
		});

		/* *************************** */

		/*
		 * Récupération des distributions
		 */
		// MAP
		JavaPairRDD<Integer, Integer> distributions = sequences.mapToPair(new PairFunction<String, Integer, Integer>() {
			public Tuple2<Integer, Integer> call(String s) {
				return new Tuple2<Integer, Integer>(s.length(), 1);
			}
		});

		// REDUCE
		JavaPairRDD<Integer, Integer> distLength = distributions
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				});

		distLength = distLength.sortByKey(false);

		/* ****************************************** */

		// <Seq,Quality>
		JavaPairRDD<String, String> SeqQualities = fqrdd.mapToPair(new PairFunction<FastqRecord, String, String>() {
			public Tuple2<String, String> call(FastqRecord r) {
				return new Tuple2<String, String>(r.getReadString(), r.getBaseQualityString());
			}
		});

		System.out.println("------------------------------");
		System.out.println("\t  ENTRY STATS");
		System.out.println("------------------------------");
		System.out.println("Sequences and Mean Quality");

		meanSequencesQualities.foreach(new VoidFunction<Tuple2<FastqRecord, Integer>>() {
			public void call(Tuple2<FastqRecord, Integer> t) {
				// TODO This was added
				int max = getMaxNucleotideQuality(t._1);
				int min = getMinNucleotideQuality(t._1);
				int Q1 = getQ1(t._1);
				int Q3 = getQ3(t._1);

				System.out.println(t._1.getReadHeader());
				System.out.println("\t Mean Quality: " + t._2);
				System.out.println("\t Max Quality: " + max);
				System.out.println("\t Min Quality: " + min);
				System.out.println("\t Q1 Position: " + Q1);
				System.out.println("\t Q3 Position: " + Q3);
			}
		});

		System.out.println("------------------------------");
		System.out.println("Nucleotides found");
		counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			public void call(Tuple2<String, Integer> t) {
				System.out.println(t._1 + " -> " + t._2);
			}
		});

		System.out.println("------------------------------");
		System.out.println("Sequences and length");
		sequences.foreach(new VoidFunction<String>() {
			public void call(String s) {
				// System.out.println("X"+ " -> " + s.length());
			}
		});

		System.out.println("------------------------------");
		System.out.println("Distributions found");

		distLength.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
			public void call(Tuple2<Integer, Integer> t) {
				System.out.println(t._1 + " -> " + t._2);
			}
		});

		System.out.println("------------------------------");
		System.out.println("Mean Quality per position!");
		System.out.println("------------------------------");

		int maxLength = distLength.first()._1;
		// final int positions[] = new int[maxLength];
		positions = new int[maxLength];
		// Arrays.fill(positions, 0);
		// positions[0] += 5;

		distLength.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
			public void call(Tuple2<Integer, Integer> t) {
				System.out.println("T1: " + t._1 + ", T2: " + t._2);
				addToArray(positions, t._1, t._2);
				// TODO toutes modification de position faite ici ne marche pas
				// o.o; faudra refactoriser
				positions[0] = 5;
			}
		});

		System.out.println("Final value in positions");
		for (int i = 0; i < maxLength; i++) {

			// System.out.println(positions[i]);
		}
		for (int i = 0; i < maxLength; i++) {
			int qual = getQualityInPosition(fqrdd, i);
			System.out.println("[" + i + "," + positions[i] + "]");
			// System.out.println("Position: " + i + " -> " + qual /
			// positions[i]);
		}
		/*
		 * final Integer positions[] = new Integer[maxLength];
		 * Arrays.fill(positions, 0);
		 * 
		 * JavaRDD<Integer> positionsRDD =
		 * sc.parallelize(Arrays.asList(positions)); JavaPairRDD<Integer, Long>
		 * positionsOccurence = positionsRDD.zipWithIndex();
		 * 
		 * positionsOccurence.foreach(new VoidFunction<Tuple2<Integer,Long>>(){
		 * public void call(Tuple2<Integer,Long> t){ int qual =
		 * getQualityInPosition(fqrdd, t._2.intValue());
		 * System.out.println("Position: " + t + " -> " + qual / t._1 ); } } );
		 * //
		 */

		/* ************************ */

	}

	private int getQualityInPosition(JavaRDD<FastqRecord> fqrdd, final int pos) {
		JavaPairRDD<Integer, Integer> meanPositionQualities = fqrdd
				.mapToPair(new PairFunction<FastqRecord, Integer, Integer>() {
					public Tuple2<Integer, Integer> call(FastqRecord fq) {
						int val = 0;
						if (fq.length() > pos)
							val = getQualityValue(fq.getBaseQualityString().charAt(pos));

						return new Tuple2<Integer, Integer>(pos, val);
					}
				});
		meanPositionQualities = meanPositionQualities.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});

		return meanPositionQualities.first()._2;
	}

	private JavaPairRDD<Integer, Integer> meanQualityInPos(JavaRDD<FastqRecord> fqrdd, final int pos) {
		JavaPairRDD<Integer, Integer> meanPositionQualities = fqrdd
				.mapToPair(new PairFunction<FastqRecord, Integer, Integer>() {
					public Tuple2<Integer, Integer> call(FastqRecord fq) {

						return new Tuple2<Integer, Integer>(pos,
								getQualityValue(fq.getBaseQualityString().charAt(pos)));
					}
				});
		return meanPositionQualities;
	}

	public JavaRDD<FastqRecord> filterFqRdd(JavaSparkContext sc, String folderPath, final int minLength,
			final int maxLength, final int minQual, final int maxQual, boolean atgc) throws IOException {
		JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc, folderPath);

		JavaRDD<FastqRecord> fltrRes = fqrdd.filter(new Function<FastqRecord, Boolean>() {
			public Boolean call(FastqRecord fq) {
				int quality = getSequenceQuality(fq);
				int len = fq.getReadString().length();
				return (len >= minLength) && (len <= maxLength) && (quality >= minQual) && (quality <= maxQual);
			}
		});

		if (atgc) {
			fltrRes = fltrRes.filter(new Function<FastqRecord, Boolean>() {
				public Boolean call(FastqRecord fq) {
					// return !fq.getReadString().contains("N"); // retourne que
					// si il y a atgc
					String seqLeft = fq.getReadString().replace("A", "").replace("T", "").replace("G", "").replace("C",
							"");
					boolean res = !(seqLeft.length() > 0); // TODO Test

					// System.out.println("SEQ LEFT!! -- ." + seqLeft +'.');
					return res;
				}
			});
		}
		return fltrRes;
	}

	public JavaRDD<FastqRecord> filterJavaRdd(JavaRDD<FastqRecord> fqrdd, final int minLength, final int maxLength,
			final int minQual, final int maxQual, boolean atgc) throws IOException {

		JavaRDD<FastqRecord> fltrRes = fqrdd.filter(new Function<FastqRecord, Boolean>() {
			public Boolean call(FastqRecord fq) {
				int quality = getSequenceQuality(fq);
				int len = fq.length();
				return (len >= minLength) && (len <= maxLength) && (quality >= minQual) && (quality <= maxQual);
			}
		});

		if (atgc) {
			fltrRes = fltrRes.filter(new Function<FastqRecord, Boolean>() {
				public Boolean call(FastqRecord fq) {
					// return !fq.getReadString().contains("N"); // retourne que
					// si il y a atgc
					String seqLeft = fq.getReadString().replace("A", "").replace("T", "").replace("G", "").replace("C",
							"");
					boolean res = !(seqLeft.length() > 0); // TODO Test

					// System.out.println("SEQ LEFT!! -- ." + seqLeft +'.');
					return res;
				}
			});
		}
		return fltrRes;
	}

	// TODO Test
	public void filterFqRddwID(JavaSparkContext sc, String folderPath, String idFilePath, final int minLength,
			final int maxLength, final int minQual, final int maxQual, boolean atgc) throws IOException {
		// JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc,folderPath);

		File headerFile = new File(idFilePath);
		final List<String> headers = Files.readAllLines(FileSystems.getDefault().getPath(idFilePath));

		JavaRDD<FastqRecord> fqrdd = filterFqRdd(sc, folderPath, minLength, maxLength, minQual, maxQual, atgc);
		JavaRDD<FastqRecord> fltrRes = fqrdd.filter(new Function<FastqRecord, Boolean>() {
			public Boolean call(FastqRecord fq) {
				return headers.contains(fq.getReadHeader());

			}
		});

		// return fltrRes;

	}

	public void sampleFqRdd(JavaSparkContext sc, String folderPath, int nb, int seed) throws IOException {
		JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc, folderPath);

		double percent = nb / fqrdd.count();
		JavaRDD<FastqRecord> fqresult = fqrdd.sample(false, percent, seed);

		// return fqresult;

	}

	public void trimFqRdd(JavaSparkContext sc, String folderPath, final int minQuality, final int windowSize)
			throws IOException {
		JavaRDD<FastqRecord> fqrdd = readFqRDDFolder(sc, folderPath);

		// filtrer sur la qualité moyenne et la longueur de la séquence

		JavaRDD<FastqRecord> fltrRes = fqrdd.filter(new Function<FastqRecord, Boolean>() {
			public Boolean call(FastqRecord fq) {
				int quality = getSequenceQuality(fq);
				return quality >= minQuality && fq.length() >= 100; // TODO
																	// Verfifier
																	// avec
																	// Kévin
			}
		});

		fltrRes.foreach(new VoidFunction<FastqRecord>() {
			public void call(FastqRecord fq) {
				int length = fq.length();

				int idBegin = 0;
				int idEnd = length - 1;
				while (idBegin < length - 1
						&& getQualityValue(fq.getBaseQualityString().charAt(idBegin)) < minQuality) {// TODO
																										// Verfifier
																										// avec
																										// Kévin
					++idBegin;
				}
				// TODO Verifier avec Kévin
				if (length - idBegin > 60) {

					idEnd = idBegin;
					while (length - idEnd - 1 > windowSize && getStringQuality(
							fq.getBaseQualityString().substring(idEnd, idEnd + windowSize)) > minQuality) {
						++idEnd;
					}
					idEnd += windowSize;

				}
				// VERIFIER SI L'OBJET PASSE EN ENTREE A ETE MODIFIE
				fq = new FastqRecord(fq.getReadHeader(), fq.getReadString().substring(idBegin, idEnd),
						fq.getBaseQualityHeader(), fq.getBaseQualityString().substring(idBegin, idEnd));

				// FastqRecord test = new FastqRecord(seqHeaderPrefix, seqLine,
				// qualHeaderPrefix, qualLine)

			}
		});

		// filtrage pour voir si les séquence obtenu du trim sont interessantes
		// ou pas
		JavaRDD<FastqRecord> last = fltrRes.filter(new Function<FastqRecord, Boolean>() {
			public Boolean call(FastqRecord fq) {

				return fq.length() >= 100; // TODO Verfifier avec Kévin
			}
		});

		last.foreach(new VoidFunction<FastqRecord>() {
			public void call(FastqRecord fq) {
				System.out.println("Sequence Header: " + fq.getBaseQualityHeader());
				System.out.println("Sequence Length: " + fq.getReadString().length());
				System.out.println("Sequence Quality: " + getStringQuality(fq.getBaseQualityString()));
				System.out.println("Sequence Length: " + fq.getReadString().length());
				System.out.println("\t Length: " + fq.length());
				System.out.println("Quality Length: " + fq.getBaseQualityString().length());
			}

		});

	}

}
