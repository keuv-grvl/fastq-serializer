# Name

Currently: `myprog`

# Purpose

## File format

Fastq is a textual format. This is inefficient to read, especially in distributed environment such as Spark.
Here we propose an alternative to this format: `fqrdd`. Basically, `fastq` files were loaded into Spark RDD then serialized.

## Companion software

The following document describes `fqrdd` file manipulation.


# Feature description

## Data conversion

### Import ([issue#10](https://github.com/keuv-grvl/fastq-serializer/issues/10))

Read standard `fastq` file to convert it to `fqrdd`.


### Export ([issue#14](https://github.com/keuv-grvl/fastq-serializer/issues/14))

Display `fqrdd` in `fastq` format. Default output is STDOUT so it could be redirected to a file.


## Data description

### Count ([issue#11](https://github.com/keuv-grvl/fastq-serializer/issues/11))

Provide basic statistics about the `fqrdd` file, such as:

- number of entries
- number of nucleotides
- number of each nucleotide (A, T, G, C , N)
- GC% ($= (COUNT(G) + COUNT(C)) / COUNT(ATGC)$)

### Quality ([issue#11](https://github.com/keuv-grvl/fastq-serializer/issues/11))

Provide information about data quality, such as:

- sequence length:
  - for each entry
  - distribution
- mean quality:
  - per entry
  - per nucleotide position for all entries
  - distribution

These data would be used to generate some graphs, so a conveniant output would be required (e.g.: tsv).


## Data processing

### Sampling ([issue#12](https://github.com/keuv-grvl/fastq-serializer/issues/12))

Randomly select (without replacement) a subset of entries. No oversampling.
The result will be output in a different `fqrdd` file.


### Trimming ([issue#15](https://github.com/keuv-grvl/fastq-serializer/issues/15))

The quality of sequencing read tends to lower while the read gets longer. 

![Boxplots of quality score per sequencing read position](http://i.imgur.com/ZEDDH.png)\

Trimming is a common practice to enhance data quality. It consists of cutting read extremities with bad quality (usually where Q<20). On Figure 1, one might keep only the 240 first nucleotides.


They are several trimming strategies. We propose to remove leading and trailing nucleotides with very poor quality (e.g.: Q<5). Then we scan the read with a sliding window, cutting when the average quality per base drops below the defined threshold. We will only focus on single end reads for the moment.


Here is an example of trimming software which use sliding windows: [Sickle](https://github.com/najoshi/sickle) (GPLv3). 

There is also a Java software, [Trimmomatic](http://www.usadellab.org/cms/?page=trimmomatic) (GPLv3), but it does more than trimming reads. Nevertheless, it may be a source of inspiration.


### Filtering ([issue#13](https://github.com/keuv-grvl/fastq-serializer/issues/13))

Some sequencing reads have to be discarded on several criteria, such as:

- read identifiers
- min and/or max read length
- min and/or max read mean quality
- nucleotide composition

Filtering by composition discards reads if they contain any other nucleotide than A, T, G or C (often labeled as N).


# Man page draft

``` 
NAME

  myprog

SYNOPSIS

  myprog {help,version}

  IMPORT/EXPORT

    myprog {import,export}

  STATISCTICS

    myprog {count,quality}

  PROCESSING

    myprog {trim,filter,sample}


DESCRIPTION

  COMMANDS
    help                        Print this help message

    version                     Print version

    import
      --input     <file.fastq>  Input file (mandatory). May be compressed with
                                gzip or bzip2
      --output    <file.fqrdd>  Output file (default: <file.fqrdd>)

    export
      --input     <file.fqrdd>  Input file (mandatory)
      --output    <file.fastq>  Output file (default: -)

    count
      --input     <file.fqrdd>  Input file (mandatory)
      --output      <file.tsv>  Output file (default: -)

      --entries                 Total number of entries
      --nucleotides             Total umber of nucleotide
      --nucl          <string>  Number of nucleotide {a,t,g,c,n} and any
                                combinason. Default: atgcn
      --all                     All of the above

    quality
      --input     <file.fqrdd>  Input file (mandatory)
      --output      <file.tsv>  Output file (default: -)

      --length                  Print length per distlength
      --dist-lenght             Print length distribution
      --per-read                Print mean quality and stddev per read
      --dist-per-read           Print quality distribution per read
      --per-position            Print mean quality and stddev per position
      --dist-per-position       Print quality distribution per position

    trim
      --input   <infile.fqrdd>  Input file (mandatory)
      --output <outfile.fqrdd>  Output file (default: <file.trim.fqrdd>)

      --leading          <int>  Remove leading bases while quality is less than <int>
      --trailing         <int>  Remove trailing bases while quality is less than <int>
      --window-size      <int>  Slinding window size. Default: 0.1*read size
      --min-qual         <int>  Minimum mean quality in the sliding window.

    filter
      --input   <infile.fqrdd>  Input file (mandatory)
      --output <outfile.fqrdd>  Output file (default: <file.filter.fqrdd>)

      --by-id     <idfile.txt>  Keep entries when ID is in idfile.txt
      --min-length       <int>  Keep entries when length is at least <int>
      --max-length       <int>  Keep entries when length is less than <int>
      --min-mean-qual    <int>  Keep entries when mean quality is at least <int>
      --max-mean-qual    <int>  Keep entries when mean quality is less than <int>
      --only-atgc               Keep entries when only ATGC are present

    sample
      --input   <infile.fqrdd>  Input file (mandatory)
      --output <outfile.fqrdd>  Output file (default: <file.sample.fqrdd>)

      --number           <int>  Select <int> entries
      --seed             <int>  Random seed
```
