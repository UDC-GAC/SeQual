package com.roi.galegot.sequal.sequalmodel.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.trimmer.Trimmer;
import com.roi.galegot.sequal.sequalmodel.trimmer.TrimmerFactory;
import com.roi.galegot.sequal.sequalmodel.trimmer.TrimmerParametersNaming;
import com.roi.galegot.sequal.sequalmodel.trimmer.Trimmers;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class TrimService.
 */
public class TrimService {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = Logger.getLogger(TrimService.class.getName());

	/**
	 * Trim.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	public JavaRDD<Sequence> trim(JavaRDD<Sequence> sequences) {
		List<Trimmers> trimmers = this.getTrimmers();
		if (!trimmers.isEmpty()) {
			return this.applyTrimmers(sequences, trimmers);
		} else {
			LOGGER.warn("\nNo trimmers specified. No operations will be performed.\n");
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
	private JavaRDD<Sequence> applyTrimmers(JavaRDD<Sequence> sequences, List<Trimmers> trimmers) {
		for (int i = 0; i < trimmers.size(); i++) {
			if (sequences.isEmpty()) {
				return sequences;
			}

			LOGGER.info("\nApplying trimmer " + trimmers.get(i));

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
	private List<Trimmers> getTrimmers() {
		String trimmers;
		String[] splitTrimmers;
		Map<Integer, Trimmers> trimmersMap;

		trimmersMap = new TreeMap<>();

		trimmers = ExecutionParametersManager.getParameter(TrimmerParametersNaming.TRIMMERS_LIST);
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