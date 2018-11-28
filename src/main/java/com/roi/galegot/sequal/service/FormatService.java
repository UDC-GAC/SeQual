package com.roi.galegot.sequal.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.formatter.Formatter;
import com.roi.galegot.sequal.formatter.FormatterFactory;
import com.roi.galegot.sequal.formatter.Formatters;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class FormatService.
 */
public class FormatService {

	/**
	 * Instantiates a new format service.
	 */
	private FormatService() {
	}

	/**
	 * Format.
	 *
	 * @param seqs the seqs
	 * @return the java RDD
	 */
	public static JavaRDD<Sequence> format(JavaRDD<Sequence> seqs) {
		List<Formatters> formatters = getFormatters();
		if (!formatters.isEmpty()) {
			return formatLoop(seqs, formatters);
		} else {
			System.out.println("\n\nNo formatters specified. No operations will be performed.");
		}
		return seqs;
	}

	/**
	 * Format loop.
	 *
	 * @param seqs       the seqs
	 * @param formatters the formatters
	 * @return the java RDD
	 */
	private static JavaRDD<Sequence> formatLoop(JavaRDD<Sequence> seqs, List<Formatters> formatters) {
		for (int i = 0; i < formatters.size(); i++) {
			Formatter formatter = FormatterFactory.getFormatter(formatters.get(i));
			seqs = formatter.format(seqs);
		}
		return seqs;
	}

	/**
	 * Gets the formatters.
	 *
	 * @return the formatters
	 */
	private static List<Formatters> getFormatters() {
		String formatters = ExecutionParametersManager.getParameter("Formatters");
		String[] splitFormatters = formatters.split("\\|");
		ArrayList<Formatters> enumFormatters = new ArrayList<>();
		if (!formatters.isEmpty()) {
			for (String formatter : splitFormatters) {
				enumFormatters.add(Formatters.valueOf(formatter.trim()));
			}
		}
		return enumFormatters;
	}
}