# SeQual
Parallel software tool that allows to perform different quality controls on large sets of genetic sequence data in an efficient way.

[![Build Status](https://travis-ci.com/roigalegot/SeQual.svg?token=az3pEmytBgoPiNLjCssG&branch=master)](https://travis-ci.com/roigalegot/SeQual)

## Getting Started

### Prerequisites

To compile and run this application you will need a valid installation of Maven and Spark.

### Compiling

To compile the app and generate its .jar, run the following command in the project folder:

```
mvn package -DskipTests
```

This will generate the needed .jar inside target/ folder.

### Running

To run the app, run the .jar located in the folder "target" with the following command:

```
spark-submit sequal.jar [options]
```

### Usage




## Features

### Filtering

SeQual has 2 kinds of filters: Single and Group. Both kind of filters remove sequences from the set based on differents parameters.

Single filters work over individual sequences, and are faster than group ones. Available single filters (in order of execution) are:

* LENGTH: Filters sequences based on a maximum and minimum sequence length threshold.
* QUALITYSCORE: Filters sequences based on a maximum and minimum threshold individual base quality score.
* QUALITY: Filters sequences based on a maximum and minimum mean quality threshold.
* GC: Filters sequences based on a maximum and minimum guanine-cytosine content threshold.
* GCP: Filters sequences based on a maximum and minimum guanine-cytosine content percentage threshold.
* NAMB: Filters sequences based on a maximum and minimum ambiguous nucleotide content threshold.
* NAMBP: Filters sequences based on a maximum and minimum ambiguous nucleotide content percentage threshold.
* NONIUPAC: Filters sequences with Non-IUPAC characters inside them.
* PATTERN: Filters sequences that contain an specified pattern.
* NOPATTERN: Filters sequences that don't contain an specified pattern.
* BASEN: Filters sequences based on a maximum and minimum base (or bases) content threshold.
* BASEP: Filters sequences based on a maximum and minimum base (or bases) content percentage threshold.


Group filters work over the whole set of sequences, implying a heavy work load. Available group filters (in order of execution) are:

* DISTINCT: Filters duplicate sequences, keeping the ones with a highest quality mean.
* ALMOSTDISTINCT: Filters duplicate sequences allowing a specified number of different bases, keeping the ones with a highest quality mean.
* REVERSEDISTINCT: Filters reverse duplicate sequences (for example, GAT and TAG), keeping the ones with a highest quality mean.
* COMPLEMENTDISTINCT: Filters complementary sequences (for example, GAT and CTA), keeping the ones with a highest quality mean.
* REVERSECOMPLEMENTDISTINCT: Filters reverse complementary sequences (for example, GAT and ATC), keeping the ones with a highest quality mean.


### Trimming

Available trimmers "cut" the sequences in different available ways:

* TRIMLEFT: Trims n positions from the sequence starting by the 5'-end (left).
* TRIMRIGHT: Trims n positions from the sequence starting by the 3'-end (right).
* TRIMLEFTP: Trims n positions from the sequence based on the specified percentage starting by the 5'-end (left).
* TRIMRIGHTP: Trims n positions from the sequence based on the specified percentage starting by the 3'-end (right).
* TRIMQUALLEFT: Trims a sequence to a specified quality mean starting by the 5'-end (left).
* TRIMQUALRIGHT: Trims a sequence to a specified quality mean starting by the 3'-end (right).
* TRIMNLEFT: Trims N-tail of specified length starting by the 5'-end (left).
* TRIMNRIGHT: Trims N-tail of specified length starting by the 3'-end (right).
* TRIMLEFTTOLENGTH: Trims a sequence to a specified length starting by the 5'-end (left).
* TRIMRIGHTTOLENGTH: Trims a sequence to a specified length starting by the 3'-end (right).

### Formatting

* DNATORNA: Transforms sequences into RNA format.
* RNATODNA: Transforms sequences into DNA format.
* FASTQTOFASTA: Transform FASTQ sequences into FASTA format.

### Measurements

* COUNT: Counts number of sequences in the set before and after performing operations.
* MEANLENGTH: Calculates mean length of the sequence set before and after performing operations.
* MEANQUALITY: Calculates mean quality of the sequence set before and after performing operations.

## Available options

Configuration options:

* -g: Generates a blank configuration file in the specified with -o location");
* -smc SparkMasterConf: Specifies Spark master configuration (local[*] by default)");
* -slc SparkLoggerConf: Specifies Spark logger configuration (ERROR by default)");

Mandatory options:

* -i InputFile: Specifies input file from where sequences will be read.
* -o OuputDirectory: Specifies output directory where resulting sequences will be written.
* -c ConfigFile: Specifies run options configuration file.

Available options:

* -f: Filters read sequences following specified parameters.
* -fo: Formats read sequences following specified parameters.
* -t: Trims read sequences following specified parameters.
* -s: Calculates statistics before and after performing transformations on the read sequences.
* -sfo: Generates a single file containing result sequences inside the output directory named {input-file-name}-results.{format}, along with a folder named Parts containing HDFS files.


## Used tools

* [Java](https://www.java.com/) - Programming Language
* [Spark](https://spark.apache.org/) - Data processor
* [Maven](https://maven.apache.org/) - Dependency Management
* [Continuous Integration](https://travis-ci.com/roigalegot/SeQual) - Continuous Integration Tool
* [HSP](https://github.com/rreye/hsp) - File Reader

## Repository

[GitHub Repository](https://github.com/roigalegot/SeQual) 

## Author

* **Roi Galego Torreiro** 

## Mentors

* **Jorge Gonz&aacute;lez Dom&iacute;nguez** 

* **Roberto Rey Exp&oacute;sito** 
