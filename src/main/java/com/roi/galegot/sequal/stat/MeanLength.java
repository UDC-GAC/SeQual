package com.roi.galegot.sequal.stat;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import com.roi.galegot.sequal.common.Sequence;

/**
 * The Class MeanLength.
 */
public class MeanLength implements Stat {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -5049048767496389502L;

	@Override
	public Double measure(JavaRDD<Sequence> seqs) {

		BigDecimal mean;
		DummyCount initialDummy = new DummyCount();
		DummyCount resultDummy;
		Function2<DummyCount, Sequence, DummyCount> addAndCount;
		Function2<DummyCount, DummyCount, DummyCount> combine;

		addAndCount = new Function2<DummyCount, Sequence, DummyCount>() {

			private static final long serialVersionUID = 1L;

			@Override
			public DummyCount call(DummyCount v1, Sequence v2)
					throws Exception {
				v1.countLength += v2.getLength();
				v1.countNumber = v1.countNumber + Long.valueOf(1);

				return v1;
			}

		};

		combine = new Function2<DummyCount, DummyCount, DummyCount>() {

			private static final long serialVersionUID = 1L;

			@Override
			public DummyCount call(DummyCount v1, DummyCount v2)
					throws Exception {
				v1.countLength += v2.countLength;
				v1.countNumber += v2.countNumber;

				return v1;
			}

		};

		resultDummy = seqs.aggregate(initialDummy, addAndCount, combine);
		mean = resultDummy.getMean();

		return mean.doubleValue();
	}

	/**
	 * The Class DummyCount.
	 */
	public class DummyCount implements Serializable {

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = -7774902974273299201L;

		/** The count length. */
		private Long countLength = Long.valueOf(0);

		/** The count number. */
		private Long countNumber = Long.valueOf(0);

		/**
		 * Gets the mean.
		 *
		 * @return the mean
		 */
		public BigDecimal getMean() {
			if (this.countNumber <= 0) {
				return BigDecimal.ZERO;
			}

			return BigDecimal.valueOf(this.countLength).divide(
					BigDecimal.valueOf(this.countNumber), 2,
					BigDecimal.ROUND_UP);
		}

	}

}