package com.roi.galegot.sequal.filter.group;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.roi.galegot.sequal.common.Sequence;

import scala.Tuple2;

public class ReverseDistinct implements GroupFilter {

	private static final long serialVersionUID = -1061447508688116490L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		JavaPairRDD<ReverseString, Sequence> group = seqs
				.mapToPair(new PairFunction<Sequence, ReverseString, Sequence>() {

					private static final long serialVersionUID = -3184033798508472514L;

					@Override
					public Tuple2<ReverseString, Sequence> call(Sequence seq) {
						return new Tuple2<ReverseString, Sequence>(new ReverseString(seq.getSeq()), seq);
					}
				});

		if (seqs.first().isHasQual()) {
			return group.reduceByKey(new Function2<Sequence, Sequence, Sequence>() {

				private static final long serialVersionUID = -1935325326481753717L;

				@Override
				public Sequence call(Sequence seq1, Sequence seq2) {
					if (seq1.getQuality() >= seq2.getQuality()) {
						return seq1;
					}
					return seq2;
				}
			}).values();
		} else {
			return group.reduceByKey(new Function2<Sequence, Sequence, Sequence>() {

				private static final long serialVersionUID = -3885295642075191150L;

				@Override
				public Sequence call(Sequence seq1, Sequence seq2) {
					return seq1;
				}
			}).values();
		}
	}

	class ReverseString implements Serializable {

		private static final long serialVersionUID = -3467393373228813336L;
		private String s, sr;

		public ReverseString(String s) {
			this.s = s;
			this.sr = new StringBuilder(s).reverse().toString();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result)
					+ (((this.s == null) ? 0 : this.s.hashCode()) + ((this.sr == null) ? 0 : this.sr.hashCode()));
			return result;
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
			ReverseString other = (ReverseString) obj;
			if (this.s == null) {
				if (other.sr != null) {
					return false;
				}
			} else if (!this.s.equals(other.sr)) {
				return false;
			}
			if (this.sr == null) {
				if (other.s != null) {
					return false;
				}
			} else if (!this.sr.equals(other.s)) {
				return false;
			}
			return true;
		}

	}

}