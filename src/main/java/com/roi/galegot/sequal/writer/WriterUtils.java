package com.roi.galegot.sequal.writer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.common.SequenceUtils;

/**
 * The Class HDFSToFile.
 */
public class WriterUtils {

	private static String OUTPUT_FOLDER_NAME_FIRST_PAIR = "/FirstPairs";
	private static String OUTPUT_FOLDER_NAME_SECOND_PAIR = "/SecondPairs";

	/**
	 * Instantiates a new HDFS to file.
	 */
	public WriterUtils() {
	}

	/**
	 * Write to file.
	 *
	 * @param folderPath     the folder path
	 * @param outputFileName the output file name
	 * @param partsFolder    the parts folder
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void mergeHDFSToFile(String folderPath, String outputFileName, String partsFolder)
			throws IOException {

		File folder = new File(partsFolder);
		File merged = new File(folderPath + "/" + outputFileName);

		if (!merged.exists()) {
			merged.createNewFile();
		}

		PrintWriter printWriter = new PrintWriter(merged);
		String[] fileStringsList = folder.list((dir, name) -> StringUtils.startsWith(name, "part-"));

		for (String fileString : fileStringsList) {
			File file = new File(folder, fileString);
			BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

			String line = "";
			while ((line = bufferedReader.readLine()) != null) {
				printWriter.println(line);
			}

			bufferedReader.close();
		}

		printWriter.flush();
		printWriter.close();
	}

	/**
	 * Write HDFS.
	 *
	 * @param sequences the sequences
	 * @param output    the output
	 */
	public static void writeHDFS(JavaRDD<Sequence> sequences, String output) {
		if (sequences.first().getIsPaired()) {
			writeHDFSPaired(sequences, output);
		} else {
			sequences.saveAsTextFile(output);
		}
	}

	/**
	 * Write HDFS and merge to file.
	 *
	 * @param sequences the sequences
	 * @param output    the output
	 * @param filename  the filename
	 * @param format    the format
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void writeHDFSAndMergeToFile(JavaRDD<Sequence> sequences, String output, String filename,
			String format) throws IOException {

		if (sequences.first().getIsPaired()) {
			String partsFolder1 = output + OUTPUT_FOLDER_NAME_FIRST_PAIR;
			String partsFolder2 = output + OUTPUT_FOLDER_NAME_SECOND_PAIR;

			writeHDFSPaired(sequences, output);

			mergeHDFSToFile(output, filename + "_1-results." + format, partsFolder1);
			mergeHDFSToFile(output, filename + "_2-results." + format, partsFolder2);
		} else {
			String partsFolder = output + "/Parts";

			writeHDFS(sequences, partsFolder);

			mergeHDFSToFile(output, filename + "-results." + format, partsFolder);
		}

	}

	/**
	 * Write HDFS paired.
	 *
	 * @param sequences the sequences
	 * @param output    the output
	 */
	private static void writeHDFSPaired(JavaRDD<Sequence> sequences, String output) {
		JavaRDD<Sequence> firstSequences;
		JavaRDD<Sequence> secondSequences;

		firstSequences = sequences.map(sequence -> SequenceUtils.getFirstSequenceFromPair(sequence));
		secondSequences = sequences.map(sequence -> SequenceUtils.getSecondSequenceFromPair(sequence));

		firstSequences.saveAsTextFile(output + OUTPUT_FOLDER_NAME_FIRST_PAIR);
		secondSequences.saveAsTextFile(output + OUTPUT_FOLDER_NAME_SECOND_PAIR);

	}

}
