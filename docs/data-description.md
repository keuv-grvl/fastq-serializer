# Data description

## Fastq files

| File | Size | Technology | Phred encoding | Number of reads | Read length |
|---|---|---|---|---|---|
| SP1.fq | 22.5 kb | Solexa | Phred+33 | 250 | 31 |
| SP2.fq | 96.1 mb | 454 | Phred+33 | 125000 | 30-1179 |
| SP3.fq | 97.3 mb | Illumina | Phred+33 | 110612 | 40-588 |
| 2genomes.fq | 151 mb | - | Phred+33 | 50000 | 1500 |

Quality reports were produced using FastQC v0.11.4.

## Other files

| File | Size | Note |
|---|---|---|
| test.txt | 56 o | 1 word/line |


# Real data

## Read simulation
[Grinder paper](https://doi.org/10.1093/nar/gks251)

``` bash
grinder \
  -reference_file "$(readlink -f genomes/ALL.fna)"  \
  -random_seed 123                                  \
  -total_reads 50000                                \
  -read_dist 1500                                   \
  -exclude_chars "NX"                               \
  -qual_levels 36 10                                \
  -fastq_output 1                                   \
  -base_name "2genomes"
```

## Expected clustering results

``` bash
echo -e "#seq.id\tcluster.id" > data/expected-clustering.tsv
zcat data/2genomes.fq.gz  \
  | grep "^@"  \
  | perl -pe 's/^@(.+?)\s.+\|(NC_.+)\|.+$/$1\t$2/g'  \
  >> data/expected-clustering.tsv
```
