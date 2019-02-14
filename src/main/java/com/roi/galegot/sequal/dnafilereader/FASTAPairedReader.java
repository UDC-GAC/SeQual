package com.roi.galegot.sequal.dnafilereader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.roi.galegot.sequal.common.Sequence;

import es.udc.gac.hadoop.sequence.parser.mapreduce.FastAInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndSequenceInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndSequenceRecordReader;

/**
 * The Class FASTAReader.
 */
@SuppressWarnings("serial")
public class FASTAPairedReader implements DNAPairedFileReader {

	/**
	 * Read file to RDD.
	 *
	 * @param inFile1 the in file 1
	 * @param inFile2 the in file 2
	 * @param sc      the sc
	 * @return the java RDD
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public JavaRDD<Sequence> readFileToRDD(String inFile1, String inFile2, JavaSparkContext sparkContext)
			throws IOException {

		Configuration hadoopConf = sparkContext.hadoopConfiguration();
		Path inputPath1 = new Path(inFile1);
		Path inputPath2 = new Path(inFile2);

//		Set left and right input paths for HSP
		PairedEndSequenceInputFormat.setLeftInputPath(hadoopConf, inputPath1, FastAInputFormat.class);
		PairedEndSequenceInputFormat.setRightInputPath(hadoopConf, inputPath2, FastAInputFormat.class);

		JavaPairRDD<LongWritable, Text> rdd = sparkContext.newAPIHadoopFile(inputPath1.getName(),
				PairedEndSequenceInputFormat.class, LongWritable.class, Text.class, hadoopConf);

		return rdd.map(tuple -> {
			String[] seq1 = PairedEndSequenceRecordReader.getLeftRead(tuple._1, tuple._2).split("\\n");
			String[] seq2 = PairedEndSequenceRecordReader.getRightRead(tuple._1, tuple._2).split("\\n");
			Sequence seq = new Sequence(seq1[0], seq1[1]);
			seq.setPairSequence(seq2[0], seq2[1]);

			return seq;
		});

	}

}