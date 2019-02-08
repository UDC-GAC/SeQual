package com.roi.galegot.sequal.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.dnafilereader.DNAFileReaderFactory;
import com.roi.galegot.sequal.util.ExecutionParametersManager;
import com.roi.galegot.sequal.writer.HDFSToFile;

/**
 * The Class AppService.
 */
public class AppService {

	/** The spark conf. */
	private static SparkConf sparkConf;

	/** The spark context. */
	private static JavaSparkContext sparkContext;

	/** The input. */
	private String input;

	/** The output. */
	private String output;

	/** The seqs. */
	private JavaRDD<Sequence> seqs;

	/**
	 * Instantiates a new app service.
	 *
	 * @param output the output
	 */
	public AppService(String output) {
		this.output = output;
	}

	/**
	 * Instantiates a new app service.
	 *
	 * @param masterConf the master conf
	 * @param input      the input
	 * @param output     the output
	 * @param configFile the config file
	 * @param logLevel   the log level
	 */
	public AppService(String masterConf, String input, String output,
			String configFile, Level sparkLogLevel) {
		this.input = input;
		this.output = output;

		this.start(masterConf, sparkLogLevel);
		ExecutionParametersManager.setConfigFile(configFile);
	}

	/**
	 * End.
	 */
	public void end() {
		this.stop();
	}

	/**
	 * Configures the Spark app and its context.
	 *
	 * @param masterConf the master conf
	 * @param logLevel   the log level
	 */
	private void start(String masterConf, Level sparkLogLevel) {
		Logger.getLogger("org").setLevel(sparkLogLevel);
		Logger.getLogger("akka").setLevel(sparkLogLevel);

		sparkConf = new SparkConf().setAppName("SeQual").setMaster(masterConf);
		sparkContext = new JavaSparkContext(sparkConf);
	}

	/**
	 * Stops the Spark app and its context.
	 */
	private void stop() {
		sparkContext.close();
	}

	/**
	 * Returns the extension of the specified file.
	 *
	 * @param file Path to the file
	 * @return String extension of the file
	 */
	private String getFormat(String file) {
		return FilenameUtils.getExtension(file);
	}

	/**
	 * Gets the file name.
	 *
	 * @param file the file
	 * @return the file name
	 */
	private String getFileName(String file) {
		return FilenameUtils.getBaseName(file);
	}

	/**
	 * Gets the actual format.
	 *
	 * @return the actual format
	 */
	private String getActualFormat() {
		if (this.seqs.first().isHasQual()) {
			return "fq";
		}

		return "fa";
	}

	/**
	 * Read.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void read() throws IOException {
		this.seqs = DNAFileReaderFactory.getReader(this.getFormat(this.input))
				.readFileToRDD(this.input, sparkContext);
	}

	/**
	 * Write.
	 *
	 * @param singleFile the single file
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void write(Boolean singleFile) throws IOException {
		if (singleFile) {
			String partsFolder = this.output + "/Parts";
			this.seqs.saveAsTextFile(partsFolder);
			HDFSToFile.writeToFile(this.output, this.getFileName(this.input)
					+ "-results." + this.getActualFormat(), partsFolder);
		} else {
			this.seqs.saveAsTextFile(this.output);
		}
	}

	/**
	 * None.
	 */
	public void none() {
		this.stop();
	}

	/**
	 * Filter.
	 */
	public void filter() {
		if (!this.seqs.isEmpty()) {
			this.seqs = FilterService.filter(this.seqs);
		}
	}

	/**
	 * Format.
	 */
	public void format() {
		if (!this.seqs.isEmpty()) {
			this.seqs = FormatService.format(this.seqs);
		}
	}

	/**
	 * Trim.
	 */
	public void trim() {
		if (!this.seqs.isEmpty()) {
			this.seqs = TrimService.trim(this.seqs);
		}
	}

	/**
	 * Measure.
	 *
	 * @param isFirst the is first
	 */
	public void measure(boolean isFirst) {
		if (!this.seqs.isEmpty()) {
			StatService.measure(this.seqs, isFirst);
		}
	}

	/**
	 * Prints the stats.
	 */
	public void printStats() {
		System.out.println(StatService.getResultsAsString());
	}

	/**
	 * Generate file.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void generateConfigFile() throws IOException {
		InputStream in = this.getClass()
				.getResourceAsStream("/ExecutionParameters.properties");

		byte[] buffer = new byte[in.available()];
		in.read(buffer);

		File targetFile = new File(
				this.output + "/ExecutionParameters.properties");
		OutputStream outStream = new FileOutputStream(targetFile);
		outStream.write(buffer);
		outStream.close();
		in.close();
	}

	/**
	 * Sets the log level.
	 *
	 * @param level the new log level
	 */
	public void setLogLevel(Level sparkLogLevel) {
		Logger.getLogger("org").setLevel(sparkLogLevel);
		Logger.getLogger("akka").setLevel(sparkLogLevel);
	}

}