package com.roi.galegot.sequal.filter.group;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.roi.galegot.sequal.common.Sequence;
import scala.Tuple2;

public class ComplementDistinct implements GroupFilter {

	private static final long serialVersionUID = -5947189310641527595L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		JavaPairRDD<ComplementString, Sequence> group = seqs
				.mapToPair(new PairFunction<Sequence, ComplementString, Sequence>() {

					private static final long serialVersionUID = -7740337890433733128L;

					@Override
					public Tuple2<ComplementString, Sequence> call(Sequence seq) {
						return new Tuple2<ComplementString, Sequence>(new ComplementString(seq.getSeq()), seq);
					}
				});

		if (seqs.first().isHasQual()) {
			return group.reduceByKey(new Function2<Sequence, Sequence, Sequence>() {

				private static final long serialVersionUID = 409867625719430118L;

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

				private static final long serialVersionUID = 6231420965110817797L;

				@Override
				public Sequence call(Sequence seq1, Sequence seq2) {
					return seq1;
				}
			}).values();
		}
	}

	class ComplementString implements Serializable {

		private static final long serialVersionUID = 3011027347808993546L;
		private String s, sr;

		public ComplementString(String s) {
			this.s = s;
			this.sr = this.getComplementary(s);
		}

		private String getComplementary(String s) {
			char[] result = new char[s.length()];
			int counter = 0;
			for (char c : this.s.toCharArray()) {
				switch (c) {
				case 'A':
					result[counter] = 'T';
					break;
				case 'G':
					result[counter] = 'C';
					break;
				case 'T':
					result[counter] = 'A';
					break;
				case 'C':
					result[counter] = 'G';
					break;
				default:
					result[counter] = c;
				}
				counter++;
			}
			return String.valueOf(result);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + this.getOuterType().hashCode();
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
			ComplementString other = (ComplementString) obj;
			if (!this.getOuterType().equals(other.getOuterType())) {
				return false;
			}
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

		private ComplementDistinct getOuterType() {
			return ComplementDistinct.this;
		}

	}

}