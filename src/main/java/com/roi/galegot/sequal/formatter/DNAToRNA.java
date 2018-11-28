package com.roi.galegot.sequal.formatter;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

public class DNAToRNA implements Formatter {

	private static final long serialVersionUID = 1507230060510674227L;

	@Override
	public JavaRDD<Sequence> format(JavaRDD<Sequence> seqs) {
		return seqs.map(s -> this.doFormat(s));
	}

	private Sequence doFormat(Sequence seq) {
		seq.setSequenceString(seq.getSequenceString().replace("T", "U"));
		return seq;
	}
}