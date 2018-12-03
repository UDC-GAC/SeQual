package com.roi.galegot.sequal.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.trimmer.Trimmer;
import com.roi.galegot.sequal.trimmer.TrimmerFactory;
import com.roi.galegot.sequal.trimmer.Trimmers;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

/**
 * The Class TrimService.
 */
public class TrimService {

	/**
	 * Instantiates a new trim service.
	 */
	private TrimService() {
	}

	/**
	 * Trim.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	public static JavaRDD<Sequence> trim(JavaRDD<Sequence> sequences) {
		List<Trimmers> trimmers = getTrimmers();
		if (!trimmers.isEmpty()) {
			return applyTrimmers(sequences, trimmers);
		} else {
			System.out.println("\n\nNo trimmers specified. No operations will be performed.");
		}
		return sequences;
	}

	/**
	 * Apply trimmers.
	 *
	 * @param sequences the sequences
	 * @param trimmers  the trimmers
	 * @return the java RDD
	 */
	private static JavaRDD<Sequence> applyTrimmers(JavaRDD<Sequence> sequences, List<Trimmers> trimmers) {
		for (int i = 0; i < trimmers.size(); i++) {
			if (sequences.isEmpty()) {
				return sequences;
			}
			Trimmer trimmer = TrimmerFactory.getTrimmer(trimmers.get(i));
			sequences = trimmer.trim(sequences);
		}
		return sequences;
	}

	/**
	 * Gets the trimmers.
	 *
	 * @return the trimmers
	 */
	private static List<Trimmers> getTrimmers() {
		String trimmers;
		String[] splitTrimmers;
		Map<Integer, Trimmers> trimmersMap;

		trimmersMap = new TreeMap<>();

		trimmers = ExecutionParametersManager.getParameter("Trimmers");
		if (StringUtils.isNotBlank(trimmers)) {
			splitTrimmers = trimmers.split("\\|");
			for (String trimmer : splitTrimmers) {
				Trimmers trimmerEntity = Trimmers.valueOf(trimmer.trim());
				trimmersMap.put(trimmerEntity.getPriority(), trimmerEntity);
			}
		}

		return new ArrayList<>(trimmersMap.values());
	}

}