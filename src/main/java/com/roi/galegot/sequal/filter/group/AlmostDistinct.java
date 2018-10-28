package com.roi.galegot.sequal.filter.group;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

import scala.Tuple2;

public class AlmostDistinct implements GroupFilter {

	private static final long serialVersionUID = 3561681376850129189L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		String sDiff = ExecutionParametersManager.getParameter("MaxDifference");
		Integer maxDiff = (sDiff.isEmpty()) ? 0 : new Integer(sDiff);

		JavaPairRDD<AlmostString, Sequence> group = seqs
				.mapToPair(seq -> new Tuple2<AlmostString, Sequence>(new AlmostString(seq.getSeq(), maxDiff), seq));

		if (seqs.first().isHasQual()) {
			return group.reduceByKey((seq1, seq2) -> {
				if (seq1.getQuality() >= seq2.getQuality()) {
					return seq1;
				}
				return seq2;
			}).values();
		} else {
			return group.reduceByKey((seq1, seq2) -> seq1).values();
		}
	}

	class AlmostString implements Serializable {

		private static final long serialVersionUID = 3595254670141051507L;
		private String s;
		private int maxDiff;

		public AlmostString(String s, int maxDiff) {
			this.s = s;
			this.maxDiff = maxDiff;
		}

		@Override
		public int hashCode() {
			return 1;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (this.getClass() != obj.getClass()) {
				return false;
			}
			AlmostString other = (AlmostString) obj;
			if (this.s == null) {
				if (other.s != null) {
					return false;
				}
			} else if (!checkSeq(other.s)) {
				return false;
			}
			return true;
		}

		private Boolean checkSeq(String s2) {
			if (this.s.length() != s2.length()) {
				return false;
			}

			if ((this.maxDiff > 0) && (this.maxDiff < this.s.length())) {
				int counter = 0;
				int diffs = 0;
				for (char c : this.s.toCharArray()) {
					if (c != s2.charAt(counter)) {
						diffs++;
					}
					if (diffs > this.maxDiff) {
						return false;
					}
					counter++;
				}
				return true;
			} else {
				return this.s.equals(s2);
			}
		}

	}
}