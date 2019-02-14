package com.roi.galegot.sequal.dnafilereader;

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.roi.galegot.sequal.common.Sequence;

/**
 * The Interface DNAPairedFileReader.
 */
public interface DNAPairedFileReader extends Serializable {

	/**
	 * Reads a file (or files) and returns a RDD of Sequences based on its content.
	 *
	 * @param inFile1 the in file 1
	 * @param inFile2 the in file 2
	 * @param jsc     Context for the Spark App
	 * @return JavaRDD<Sequence> created from file (or files) content
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public JavaRDD<Sequence> readFileToRDD(String inFile1, String inFile2, JavaSparkContext sparkContext)
			throws IOException;

}