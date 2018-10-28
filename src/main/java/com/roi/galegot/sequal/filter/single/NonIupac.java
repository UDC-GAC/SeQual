package com.roi.galegot.sequal.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

public class NonIupac implements SingleFilter {

	private static final long serialVersionUID = -7681154534922631509L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		String[] bases = { "A", "C", "G", "T", "N" };
		return seqs.filter(s -> this.filter(s, bases));
	}

	private Boolean filter(Sequence seq, String[] bases) {
		int counter = 0;
		String sequence = seq.getSeq();
		for (String base : bases) {
			counter = counter + StringUtils.countMatches(sequence, base);
		}
		return (counter == sequence.length());
	}
}