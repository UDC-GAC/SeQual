package com.roi.galegot.sequal.formatter;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

public class RNAToDNA implements Formatter {

	private static final long serialVersionUID = 3160351422137802821L;

	@Override
	public JavaRDD<Sequence> format(JavaRDD<Sequence> seqs) {
		return seqs.map(s -> this.doFormat(s));
	}

	private Sequence doFormat(Sequence seq) {
		seq.setSequenceString(seq.getSequenceString().replace("U", "T"));
		return seq;
	}
}