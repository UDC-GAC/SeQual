package com.roi.galegot.sequal.writer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.lang3.StringUtils;

/**
 * The Class HDFSToFile.
 */
public class HDFSToFile {

	/**
	 * Instantiates a new HDFS to file.
	 */
	public HDFSToFile() {
	}

	/**
	 * Write to file.
	 *
	 * @param folderPath     the folder path
	 * @param outputFileName the output file name
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void writeToFile(String folderPath, String outputFileName,
			String partsFolder) throws IOException {

		File folder = new File(partsFolder);
		File merged = new File(folderPath + "/" + outputFileName);

		if (!merged.exists()) {
			merged.createNewFile();
		}

		PrintWriter printWriter = new PrintWriter(merged);
		String[] fileStringsList = folder
				.list((dir, name) -> StringUtils.startsWith(name, "part-"));

		for (String fileString : fileStringsList) {
			File file = new File(folder, fileString);
			BufferedReader bufferedReader = new BufferedReader(
					new FileReader(file));

			String line = "";
			while ((line = bufferedReader.readLine()) != null) {
				printWriter.println(line);
			}

			bufferedReader.close();
		}

		printWriter.flush();
		printWriter.close();
	}

}
