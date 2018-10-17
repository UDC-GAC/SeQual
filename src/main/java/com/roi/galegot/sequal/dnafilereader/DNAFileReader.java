package com.roi.galegot.sequal.dnafilereader;

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.roi.galegot.sequal.common.Sequence;

public interface DNAFileReader extends Serializable {

	/**
	 * Reads a file (or files) and returns a RDD of Sequences based on its content
	 *
	 * @param args Parameters received at main call, important not to modify them
	 * @param jsc  Context for the Spark App
	 * @return JavaRDD<Sequence> created from file (or files) content
	 * @throws IOException
	 */
	public JavaRDD<Sequence> readFileToRDD(String inFile, JavaSparkContext jsc) throws IOException;

}