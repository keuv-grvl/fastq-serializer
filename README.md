# Intro

[Fastq](https://en.wikipedia.org/wiki/FASTQ_format) files are widely used in bioinformatics to store raw sequencing data.
One fastq entry contains information about sequencing machine, nucleotides (A, T, C, G or N) and
[sequencing quality](https://en.wikipedia.org/wiki/Phred_quality_score).
They are textual file organized as follow:

```
@IDENTIFIER.1 various_info_about_sequencing_machine_for_example
ATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGC
+
IIIIIIIIIIIIIIIIIIIIIIIIIIIIII9IG9ICIIIIIIIIIIIIIIIIIIIIDIIIIIII>IIIIII/
```

Drastic drop in [sequencing cost](https://www.genome.gov/sequencingcosts/) leads computinal biologists to the Big Data playground.
Now imagine the fastq format for millions or billions entries.
This textual format is incredibly inefficient on ~~modern~~ computers (with more than 1 CPU).

Besides, tremendous efforts have been made to handle the data deluge.
One of the most recent example is the Apache Spark ecosystem.
It distributes tasks on an arbitrary number of nodes from GPU-enabled super computers to commodity PC.

Further development in bioinformatics may require such infrastructure, so we need to transform the old fastq standard to a new Spark-friendly format.


# Run the project

First, you will need Git, Java and Maven. This project was set up with Git 2.7.4, Java 1.8 and Maven 3.3.9.

Then,
``` bash
git clone https://github.com/keuv-grvl/fastq-serializer.git
cd fastq-serializer/
mvn clean compile package assembly:single
java -cp target/fastqserializer-*-jar-with-dependencies.jar fr.isima.fastqserializer.HelloSpark
# or
java -jar target/fastqserializer-*-jar-with-dependencies.jar
```

# Unordered resources

## Formats and tools

- [FASTQ format specification](http://dx.doi.org/10.1093/nar/gkp1137)
- [BAM/SAM format specification](http://samtools.github.io/hts-specs/SAMv1.pdf) (PDF)
- [Checking and manipulating FASTQ files](http://homer.salk.edu/homer/basicTutorial/fastqFiles.html)
- [BioJava](https://github.com/biojava/biojava)
- [FastQC](http://www.bioinformatics.babraham.ac.uk/projects/fastqc/)
- [Trimmomatic](http://www.usadellab.org/cms/?page=trimmomatic)

## Spark-related

- [Spark documentation](http://spark.apache.org/docs/latest/)
- [Setting up Spark with Maven](https://sparktutorials.github.io/2015/04/02/setting-up-a-spark-project-with-maven.html)
- [Spark 2.0.1 Docker container](https://github.com/gettyimages/docker-spark) with Java 8 and Hadoop 2.7.2
- [ADAM: BAM/SAM serialization using Apache Avro](https://github.com/bigdatagenomics/adam)
- [Understanding how Parquet integrates with Avro, Thrift and Protocol Buffers](http://grepalex.com/2014/05/13/parquet-file-format-and-object-model/)
- [Changing Spark's default Java serialization to Kryo](https://ogirardot.wordpress.com/2015/01/09/changing-sparks-default-java-serialization-to-kryo/)
- [Writing efficient Spark jobs](http://fdahms.com/2015/10/04/writing-efficient-spark-jobs/)

___

[![Join the chat at https://gitter.im/fastq-serializer/Lobby](https://badges.gitter.im/fastq-serializer/Lobby.svg)](https://gitter.im/fastq-serializer/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
