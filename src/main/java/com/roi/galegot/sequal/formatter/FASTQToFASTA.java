package com.roi.galegot.sequal.formatter;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;

public class FASTQToFASTA implements Formatter {

	private static final long serialVersionUID = -5337524582924746654L;

	@Override
	public JavaRDD<Sequence> format(JavaRDD<Sequence> seqs) {
		return seqs.map(s -> this.doFormat(s));
	}

	private Sequence doFormat(Sequence seq) {
		seq.setName(">" + seq.getName().substring(1));
		seq.setQualityString("");
		return seq;
	}
}