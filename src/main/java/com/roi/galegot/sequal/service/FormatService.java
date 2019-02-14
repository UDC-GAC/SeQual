package com.roi.galegot.sequal.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.console.ConsoleInterface;
import com.roi.galegot.sequal.formatter.Formatter;
import com.roi.galegot.sequal.formatter.FormatterFactory;
import com.roi.galegot.sequal.formatter.Formatters;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class FormatService.
 */
public class FormatService {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = Logger
			.getLogger(ConsoleInterface.class.getName());

	/**
	 * Format.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	public JavaRDD<Sequence> format(JavaRDD<Sequence> sequences) {
		List<Formatters> formatters = this.getFormatters();
		if (!formatters.isEmpty()) {
			return this.applyFormatters(sequences, formatters);
		} else {
			LOGGER.warn(
					"\nNo formatters specified. No operations will be performed.\n");
		}
		return sequences;
	}

	/**
	 * Format loop.
	 *
	 * @param sequences  the sequences
	 * @param formatters the formatters
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> applyFormatters(JavaRDD<Sequence> sequences,
			List<Formatters> formatters) {
		for (int i = 0; i < formatters.size(); i++) {
			Formatter formatter = FormatterFactory
					.getFormatter(formatters.get(i));

			LOGGER.info("Applying formatter " + formatters.get(i));

			sequences = formatter.format(sequences);
		}
		return sequences;
	}

	/**
	 * Gets the formatters.
	 *
	 * @return the formatters
	 */
	private List<Formatters> getFormatters() {
		String formatters = ExecutionParametersManager
				.getParameter("Formatters");
		ArrayList<Formatters> enumFormatters = new ArrayList<>();

		if (StringUtils.isNotBlank(formatters)) {
			String[] splitFormatters = formatters.split("\\|");
			for (String formatter : splitFormatters) {
				enumFormatters.add(Formatters.valueOf(formatter.trim()));
			}
		}

		return enumFormatters;
	}
}