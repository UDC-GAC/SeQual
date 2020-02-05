[![Build Status](https://travis-ci.com/roigalegot/SeQual.svg?token=az3pEmytBgoPiNLjCssG&branch=master)](https://travis-ci.com/roigalegot/SeQual)

# SeQual
SeQual is a parallel tool that allows to perform various quality controls, as well as transformations, on genomic sequence datasets in an efficient way. It is oriented to work with massive amounts of data or Big Data in distributed environments, looking forward to offer the best performance, and uses Apache Spark to manage such distributed data processing.


## Getting Started

SeQual can be used both on Windows and UNIX-based systems. Nevertheless, to be able to compile and use SeQual, you need a valid installation of the following:

* **Java Development Kit (JDK)** version 1.8 or above. 
* **Apache Maven** version 3.0 or above. 
* **Apache Spark** version 2.0 or above. 
* **Hadoop Sequence Parser (HSP)** version 1.0 or above.


To compile the application, you just need to use the command:

```
mvn install -DskipTests
```

This will generate a folder called /target inside the three modules of the project (SeQual-Model, SeQual-CMD and SeQual-GUI), containing each one the appropiated jar file. How to use each module is explained below.


## Features

SeQual offers mainly four groups of features or operations to apply to the datasets, grouped based on the operation's objective. These groups are:

* **Filters**: They remove the sequences that doesn't comply with the specified thresholds. 
* **Trimmers**: They trim the sequences following specified parameters.
* **Formatters**: They apply format transformations to the sequences.
* **Statistics**: They measure and calculate different dataset statistics. 
    
Besides the previous mentioned groups, there are other features grouped under the name **Transversals**, which allow the user to configure the application more thoroughly, specifying aspects like the log level or Spark's configuration.


### Available Filters

* **LENGTH**: Filters sequences based on an indicated maximum and/or minimum length threshold.
* **QUALITYSCORE**: Filters sequences based on an indicated maximum and/or minimum quality score per base threshold, removing them if any of its bases is outside the threshold. Quality score from each base is calculated following Illumina encoding.
* **QUALITY**: Filters sequences based on an indicated maximum and/or minimum mean quality threshold. Quality score from each base is calculated following Illumina encoding.
* **GCBASES**: Filters sequences based on an indicated maximum and/or minimum quantity of G(uanine) and C(ytosine) bases threshold.
* **GCCONTENT**: Filters sequences based on an indicated maximum and/or minimum GC-content threshold.
* **NAMB**: Filters sequences based on an indicated maximum and/or minimum N-ambiguous bases quantity threshold.
* **NAMBP**: Filters sequences based on an indicated maximum and/or minimum N-ambiguous bases percentage threshold.
* **NONIUPAC**: Filters sequences if they contain Non-IUPAC bases (that is, any base other than A, T, G, C or N).
* **PATTERN**: Filters sequences according to the absence of a specified pattern (that is, if it does not contain the pattern, the sequence is removed) along with its repetitions (for example, the pattern ATC with two repeats would be ATCATC).
* **NOPATTERN**: Filters sequences according to the existence of a specified pattern (that is, if it does not contain the pattern, the sequence is removed) along with its repetitions (for example, the pattern ATC with two repeats would be ATCATC).
* **BASEN**: Filters sequences according to whether they contain a maximum and/or minimum number of one or several base types (or even base groups).
* **BASEP**: Filters sequences according to whether they contain a maximum and/or minimum number of one or several base types (or even base groups).

* **DISTINCT**: Filters duplicated sequences maintaining the ones with the highest quality (if they have associated quality).
* **ALMOSTDISTINCT**: Filters duplicated sequences maintaining the ones with the highest quality (if they have associated quality), allowing an indicated margin of differences (for example, two sequences with 2 differents bases can be considered equals if the specified limit allows it).
* **REVERSEDISTINCT**: Filters reverse sequences maintaining the ones with the highest quality (if they have associated quality). For example, the reverse sequence of ATG is GTA.
* **COMPLEMENTDISTINCT**: Filters complementary sequences maintaining the ones with the highest quality (if they have associated quality). For example, the complementary sequence of ATG is TAC.
* **REVERSECOMPLEMENTDISTINCT**: Filters reverse complementary sequences maintaining the ones with the highest quality (if they have associated quality). For example, the reverse sequence of ATG is CAT.


### Available Trimmers

* **TRIMLEFT**: Trims sequences according to an indicated number of positions  starting from the 5'-end (left).
* **TRIMRIGHT**: Trims sequences according to an indicated number of positions  starting from the 3'-end (right).
* **TRIMLEFTP**: Trims sequences according to an indicated percentage of the total number of bases starting from the 5'-end (left). 
* **TRIMRIGHTP**: Trims sequences according to an indicated percentage of the total number of bases starting from the 3'-end (right).
* **TRIMQUALLEFT**: Trims sequences until achieving an indicated mean sequence quality starting from the 5'-end (left).
* **TRIMQUALRIGHT**: Trims sequences until achieving an indicated mean sequence quality starting from the 3'-end (right).
* **TRIMNLEFT**: Trims N-terminal tails with a specified minimum length at the 5'-end (left). A N-terminal tail is a set of N bases found at the beginning or end of a sequence. For example, the three Ns of the sequence NNATCGAT form a N-terminal tail at the beginning.
* **TRIMNRIGHT**: Trims N-terminal tails with a specified minimum length at the 3'-end (right).
* **TRIMLEFTTOLENGTH**: Trims sequences to a specified maximum length starting from the 5'-end (left).
* **TRIMRIGHTTOLENGTH**: Trims sequences to a specified maximum length starting from the 3'-end (right).

### Available Formatters

* **DNATORNA**: Transforms DNA sequences to RNA sequences.
* **RNATODNA**: Transforms RNA sequences to DNA sequences.
* **FASTQTOFASTA**: Transforms sequences in FASTQ format to FASTA format. In this case base the information of the quality is lost.

### Available Statistics

* **COUNT**: Calculates the total number of sequences in the dataset before and after performing the indicated operations on them.
* **MEANLENGTH**: Calculates the mean length of the sequences in the dataset before and after performing the indicated operations on them.
* **MEANQUALITY**: Calculates the mean quality of the sequences in the dataset before and after performing the indicated operations on them.

### Available Transversals

* **Reading of FASTA format datasets:** Allows to read datasets of sequences in FASTA format.
* **Reading of FASTQ format datasets:** Allows to read datasets of sequences in FASTA format.
* **Reading of paired-end FASTA format datasets:** Allows to read datasets of paired-end sequences in FASTA format. The sequences must be separated in two different files.
* **Reading of paired-end FASTQ format datasets:** Allows to read datasets of paired-end sequences in FASTA format. The sequences must be separated in two different files.
* **Writing of resulting sequences:** Allows to write the resulting sequences after the operations in the indicated route, generating two different folders in case of paired-end datasets. This type of writing is done by default, writing the result in several text files.
* **Writing of resulting sequences to an individual file:** Allows to write the resulting sequences after operations to an individual file (with the same format as the input) in the indicated path, or to two files in case of paired-end datasets.
* **Configure Apache Spark's execution mode:** Allows to configure Spark's execution mode, being local[*] by default (which implies using all the available cores in the machine where the work is executed).
* **Configure the level of log shown to the user:** Allows to configure the log level shown to the user by Spark and other libraries. The default level is ERROR.
* **Generation and reading of a parameter specification file:** Allows to generate a template file where the operations to be carried out can be specified, as well as the necessary parameters for them.
    
## How to use SeQual-CMD

SeQual-CMD allows you to run jobs from a console interface. To do this, you just need to use Spark's command spark-submit with SeQual-CMD's jar and its options. For example, a valid run would be:

```
spark-submit sequal-cmd.jar -i myfile.fastq -o myfolder -c myconfig.properties -f
```

To specify which operations you want to perform, along with its necessary parameters, you need to use a configuration file. SeQual provides a way to generate this file (option -g) with all the possible fields ready to be completed. This way, you can have multiple configuration files for different jobs, avoiding having to write the values every time you want to run a job.

Configuration options:
* **-i InputFile:** Specifies input file from where sequences will be read.
* **-di InputFile:** Specifies second input file from where sequences will be read, in case of paired-end sequences.
* **-o OuputDirectory:** Specifies output directory where resulting sequences will be written.
* **-c ConfigFile:** Specifies parameters configuration file.
* **-smc SparkMasterConf:** Specifies Spark master configuration (local[*] by default).
* **-lc LoggerConfLevel:** Specifies logger configuration for Spark and other libraries (ERROR by default).

Available options:
* **-g:** Generates a blank configuration file in the specified with -o location.
* **-f:** Filters read sequences following specified parameters.
* **-fo:** Formats read sequences following specified parameters.
* **-t:** Trims read sequences following specified parameters.
* **-s:** Calculates statistics before and after performing transformations on the read sequences.
* **-sfo:** Generates a single file containing result sequences inside the output directory named {input-file-name}-results.{format}, along with a folder named Parts containing HDFS files.


## How to use SeQual-GUI

SeQual-GUI allows to run jobs through a graphic user interface rather than through a console, simplifying its use. To run SeQual-GUI, a double click on the jar is enough, although it is recommended to use the Spark's command spark-submit. In this case, unlike SeQual-CMD, no arguments are needed to run it, just the following:

```
spark-submit sequal-gui.jar
```

The interface is shown in the following picture. 

![](doc/interfazexpl.png)

The interface is mainly composed by 6 different fields:

* **1: Configuration section.** Allows the user to specify different parameters, like the input file, the output folder, the level of log...
* **2: Filters section.** Allows the user to select which filters should be applied, as well as a place to specify the parameters they need.
* **3: Trimmers section.** Allows the user to select which trimmers should be applied, as well as a place to specify the parameters they need.
* **4: Formmatters sections.** Allows the user to select which formatters should be applied.
* **5: Statistics sections.** Allows the user to select which statistics should be measured.
* **6: Output section.** Console-like window used to show information to the user.


## Used tools

* [Java](https://www.java.com/) - Programming Language
* [Apache Spark](https://spark.apache.org/) - Data processor
* [Hadoop Sequence Parser](https://github.com/rreye/hsp) - File Reader
* [Apache Maven](https://maven.apache.org/) - Dependency Management
* [JavaFX](https://openjfx.io/) - Graphic User Interface
* [Travis CI](https://travis-ci.com/roigalegot/SeQual) - Continuous Integration Tool

## Repository

[GitHub Repository](https://github.com/roigalegot/SeQual) 

## Author

* **Roi Galego Torreiro** (https://www.linkedin.com/in/roi-galego)

## Mentors

* **Jorge Gonz&aacute;lez Dom&iacute;nguez** (http://gac.udc.es/~jgonzalezd)

* **Roberto Rey Exp&oacute;sito** (http://gac.udc.es/~rreye)

## License

SeQual is distributed as free software and is publicly available under the GNU GPLv3 license (see the [LICENSE](LICENSE) file for more details).
