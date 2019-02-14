package com.roi.galegot.sequal.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.dnafilereader.DNAFileReaderFactory;
import com.roi.galegot.sequal.util.ExecutionParametersManager;
import com.roi.galegot.sequal.writer.WriterUtils;

/**
 * The Class AppService.
 */
public class AppService {

	private static SparkConf sparkConf;
	private static JavaSparkContext sparkContext;

	private String input;
	private String secondInput;
	private String output;

	private JavaRDD<Sequence> sequences;

	private FilterService filterService;
	private TrimService trimService;
	private StatService statService;
	private FormatService formatService;

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
	public AppService(String masterConf, String input, String output, String configFile, Level sparkLogLevel) {
		this.input = input;
		this.output = output;

		this.filterService = new FilterService();
		this.trimService = new TrimService();
		this.statService = new StatService();
		this.formatService = new FormatService();

		this.start(masterConf, sparkLogLevel);
		ExecutionParametersManager.setConfigFile(configFile);
	}

	public void setSecondInput(String secondInput) {
		this.secondInput = secondInput;
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
		if (this.sequences.first().getHasQuality()) {
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
		if (StringUtils.isNotBlank(this.secondInput)) {
			this.sequences = DNAFileReaderFactory.getPairedReader(this.getFormat(this.input)).readFileToRDD(this.input,
					this.secondInput, sparkContext);
		} else {
			this.sequences = DNAFileReaderFactory.getReader(this.getFormat(this.input)).readFileToRDD(this.input,
					sparkContext);
		}

	}

	/**
	 * Write.
	 *
	 * @param singleFile the single file
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void write() throws IOException {
		WriterUtils.writeHDFS(this.sequences, this.output);
	}

	/**
	 * Write.
	 *
	 * @param singleFile the single file
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void writeWithSingleFile() throws IOException {
		WriterUtils.writeHDFSAndMergeToFile(this.sequences, this.output, this.getFileName(this.input),
				this.getActualFormat());
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
		if (!this.sequences.isEmpty()) {
			this.sequences = this.filterService.filter(this.sequences);
		}
	}

	/**
	 * Format.
	 */
	public void format() {
		if (!this.sequences.isEmpty()) {
			this.sequences = this.formatService.format(this.sequences);
		}
	}

	/**
	 * Trim.
	 */
	public void trim() {
		if (!this.sequences.isEmpty()) {
			this.sequences = this.trimService.trim(this.sequences);
		}
	}

	/**
	 * Measure.
	 *
	 * @param isFirst the is first
	 */
	public void measure(boolean isFirst) {
		if (!this.sequences.isEmpty()) {
			this.statService.measure(this.sequences, isFirst);
		}
	}

	/**
	 * Prints the stats.
	 */
	public void printStats() {
		System.out.println(this.statService.getResultsAsString());
	}

	/**
	 * Generate file.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void generateConfigFile() throws IOException {
		InputStream in = this.getClass().getResourceAsStream("/ExecutionParameters.properties");

		byte[] buffer = new byte[in.available()];
		in.read(buffer);

		File targetFile = new File(this.output + "/ExecutionParameters.properties");
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