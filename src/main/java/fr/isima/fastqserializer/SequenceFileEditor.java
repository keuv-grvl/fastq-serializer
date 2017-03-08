package fr.isima.fastqserializer;

import fr.isima.entities.KmeanMatrix;
import fr.isima.fastxrecord.*;

import java.io.IOException;
import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SequenceFileEditor implements Serializable {

	/**
	 * Filters an JavaRdd of FqRecords according to criterias specified
	 * 
	 * @param fqrdd:
	 *            JavaRDD to filter
	 * @param minLength:
	 *            minimum sequence length wanted
	 * @param maxLength:
	 *            maximum sequence length wanted
	 * @param minQual:
	 *            minimum sequence length wanted
	 * @param maxQual:
	 *            maximum sequence length wanted
	 * @param atgc:
	 *            specifies if the sequence should only have atgc nucleotides
	 * @return : the filtered version of the JavaRDD
	 * @throws IOException
	 *             IOException
	 */
	public JavaRDD<FastqRecord> filterJavaRdd(JavaRDD<FastqRecord> fqrdd, final int minLength, final int maxLength,
			final int minQual, final int maxQual, boolean atgc) throws IOException {

		JavaRDD<FastqRecord> fltrRes = fqrdd.filter(new Function<FastqRecord, Boolean>() {
			public Boolean call(FastqRecord fq) {
				int quality = fq.getMeanQuality();
				int len = fq.getLength();
				return (len >= minLength) && (len <= maxLength) && (quality >= minQual) && (quality <= maxQual);
			}
		});

		if (atgc) {
			fltrRes = fltrRes.filter(new Function<FastqRecord, Boolean>() {
				public Boolean call(FastqRecord fq) {
					// return !fq.getReadString().contains("N"); // retourne que
					// si il y a atgc
					String seqLeft = fq.getSequenceString().replace("A", "").replace("T", "").replace("G", "")
							.replace("C", "");
					boolean res = !(seqLeft.length() > 0); // TODO Test

					// System.out.println("SEQ LEFT!! -- ." + seqLeft +'.');
					return res;
				}
			});
		}
		return fltrRes;
	}

	/**
	 * Returns a random arrangement of sequences in an FqRdd
	 * 
	 * @param fqrdd:
	 *            RDD to sample
	 * @param nb:
	 *            number of sequences wanted
	 * @param seed:
	 *            seed for the shuffle
	 * @return the sample
	 * @throws IOException
	 *             IOException
	 */
	public JavaRDD<FastqRecord> sampleFqRdd(JavaRDD<FastqRecord> fqrdd, int nb, int seed) throws IOException {
		double percent = nb / fqrdd.count();
		JavaRDD<FastqRecord> fqresult = fqrdd.sample(false, percent, seed);

		return fqresult;

	}

	/**
	 * Trims a Sequences according to the criterias given
	 * 
	 * @param fqrdd:
	 *            RDD to trim
	 * @param minQuality:
	 *            minimum quality each sequence should have at the end
	 * @param windowSize:
	 *            windowSize of the Quality Search
	 * @return : returns the trimmed sequences
	 * @throws IOException
	 *             IOException
	 * 
	 */
	public JavaRDD<FastqRecord> trimFqRdd(JavaRDD<FastqRecord> fqrdd, final int minQuality, final int windowSize)
			throws IOException {

		// filtrer sur la qualité moyenne et la longueur de la séquence

		JavaRDD<FastqRecord> fltrRes = fqrdd.filter(new Function<FastqRecord, Boolean>() {
			public Boolean call(FastqRecord fq) {
				int quality = fq.getMeanQuality();
				return quality >= minQuality && fq.getLength() >= 100; // TODO
																		// Verfifier
																		// avec
																		// Kévin
			}
		});

		fltrRes.foreach(new VoidFunction<FastqRecord>() {
			public void call(FastqRecord fq) {
				int length = fq.getLength();

				int idBegin = 0;
				int idEnd = length - 1;
				while (idBegin < length - 1
						&& fq.getEncoding().getQualityValue(fq.getQualityString().charAt(idBegin)) < minQuality) {
					// TODO Verfifier avec Kévin
					++idBegin;
				}
				// TODO Verifier avec Kévin
				if (length - idBegin > 60) {

					idEnd = idBegin;
					while (length - idEnd - 1 > windowSize && fq.getEncoding()
							.getQualityValue(fq.getQualityString().substring(idEnd, idEnd + windowSize)) > minQuality)

					{
						++idEnd;
					}
					idEnd += windowSize;

				}
				// VERIFIER SI L'OBJET PASSE EN ENTREE A ETE MODIFIE
				fq = new FastqRecord(fq.getSequenceHeader(), fq.getSequenceString().substring(idBegin, idEnd),
						fq.getQualityHeader(), fq.getQualityString().substring(idBegin, idEnd));

				// FastqRecord test = new FastqRecord(seqHeaderPrefix, seqLine,
				// qualHeaderPrefix, qualLine)

			}
		});

		// filtrage pour voir si les séquence obtenu du trim sont interessantes
		// ou pas
		JavaRDD<FastqRecord> last = fltrRes.filter(new Function<FastqRecord, Boolean>() {
			public Boolean call(FastqRecord fq) {

				return fq.getLength() >= 100; // TODO Verfifier avec Kévin
			}
		});

		return last;
	}

	public void countAllKmers(JavaRDD<FastqRecord> fq) {

		fq.foreach(new VoidFunction<FastqRecord>() {
			public void call(FastqRecord fq) {
				// getting sequence to be analyzed
				String sequence = fq.getSequenceString();
				// getting kmer length
				int k = KmeanMatrix.getComplexity();

				// adding the current sequence to the matrix
				KmeanMatrix.getInstance().addSequence(fq.getSequenceHeader());

				// counting the different k-mers
				int i = 0;
				while (sequence.length() - i >= k) {
					String kmer = sequence.substring(i, i + k);
					KmeanMatrix.getInstance().add(fq.getSequenceHeader(), kmer);
					i++;
				}

			}
		});

	}

	public void doKmeansClusterisation(String filepath, int nbClusters) {
		// clusterisation
		// *
		SparkSession ss = SparkSession.builder().appName("GettingK-Means").getOrCreate();
		Dataset<Row> dataset = ss.read().format("libsvm").load(filepath);
		dataset.printSchema();
		dataset.show();
		// *
		// Trains a k-means model.
		KMeans kmeans = new KMeans().setK(nbClusters).setSeed(1L);
		KMeansModel model = kmeans.fit(dataset);

		// Evaluate clustering by computing Within Set Sum of Squared Errors.
		double WSSSE = model.computeCost(dataset);
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

		// Shows the result.
		Vector[] centers = model.clusterCenters();
		System.out.println("Cluster Centers: ");
		for (Vector center : centers) {
			System.out.println(center);
		}
		// */
	}

}
