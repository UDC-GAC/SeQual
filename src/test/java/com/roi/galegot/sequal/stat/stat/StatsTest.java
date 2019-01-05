package com.roi.galegot.sequal.stat.stat;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.roi.galegot.sequal.common.Sequence;
import com.roi.galegot.sequal.stat.MeanLength;
import com.roi.galegot.sequal.stat.Stat;

public class StatsTest {
	private static SparkConf spc;
	private static JavaSparkContext jsc;

	private static String commLine = "+";

	@BeforeClass
	public static void setupSpark() throws IOException {
		spc = new SparkConf().setAppName("SeQual").setMaster("local[*]");
		jsc = new JavaSparkContext(spc);
		jsc.setLogLevel("ERROR");
	}

	@AfterClass
	public static void stopSpark() {
		jsc.close();
	}

	@Test
	public void statCount() {

	}

	@Test
	public void statMeanLength() {
		/*
		 * Sequence 1 Length = 30
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * Sequence 2 Length = 29
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * Sequence 3 Length = 28
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq3s4 = ">=2.660/?:36AD;0<14703640334";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		JavaRDD<Sequence> original1 = jsc.parallelize(Arrays.asList(seq1));
		JavaRDD<Sequence> original2 = jsc
				.parallelize(Arrays.asList(seq1, seq2));
		JavaRDD<Sequence> original3 = jsc
				.parallelize(Arrays.asList(seq1, seq2, seq3));

		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		Double result;
		Stat stat = new MeanLength();

		// Test for empty RDD
		result = stat.measure(emptyRdd);
		assertTrue(result == 0);

		result = stat.measure(original1);
		assertTrue(result == 30);

		result = stat.measure(original2);
		assertTrue(result == 29.5);

		result = stat.measure(original3);
		assertTrue(result == 29);
	}

	@Test
	public void statMeanQuality() {

	}
}