package com.roi.galegot.sequal.filter.group;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.roi.galegot.sequal.common.Sequence;

import scala.Tuple2;

public class ReverseComplementDistinct implements GroupFilter {

	private static final long serialVersionUID = -3416801873476204891L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> seqs) {
		JavaPairRDD<ReverseComplementString, Sequence> group = seqs
				.mapToPair(new PairFunction<Sequence, ReverseComplementString, Sequence>() {

					private static final long serialVersionUID = -6192877628808422227L;

					@Override
					public Tuple2<ReverseComplementString, Sequence> call(Sequence seq) {
						return new Tuple2<ReverseComplementString, Sequence>(new ReverseComplementString(seq.getSeq()),
								seq);
					}
				});

		if (seqs.first().isHasQual()) {
			return group.reduceByKey(new Function2<Sequence, Sequence, Sequence>() {

				private static final long serialVersionUID = -2285412703072618941L;

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

				private static final long serialVersionUID = -6498038456070131065L;

				@Override
				public Sequence call(Sequence seq1, Sequence seq2) {
					return seq1;
				}
			}).values();
		}
	}

	class ReverseComplementString implements Serializable {

		private static final long serialVersionUID = -7032196466156045735L;
		private String s, sr;

		public ReverseComplementString(String s) {
			this.s = s;
			this.sr = new StringBuilder(this.getComplementary(s)).reverse().toString();
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
			ReverseComplementString other = (ReverseComplementString) obj;
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