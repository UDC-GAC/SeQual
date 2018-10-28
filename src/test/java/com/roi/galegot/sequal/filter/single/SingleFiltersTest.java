package com.roi.galegot.sequal.filter.single;

import static org.junit.Assert.assertEquals;
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
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class SingleFiltersTest {

	private static SparkConf spc;
	private static JavaSparkContext jsc;

	private static String commLine = "+";

	@BeforeClass
	public static void setupSpark() throws IOException {
		spc = new SparkConf().setAppName("TFG").setMaster("local[*]");
		jsc = new JavaSparkContext(spc);
		jsc.setLogLevel("ERROR");
	}

	@AfterClass
	public static void stopSpark() {
		jsc.close();
	}

	@Test
	public void filterNoPattern() {
		/*
		 * AT = 1
		 */
		String seq1s1 = "@cluster_2:UMI_ATTCCG";
		String seq1s2 = "TTTCCGGGGCACATTTTCTTCAGCCGGGCGC";
		String seq1s4 = "9C;=;=<9@4868>9:67AA<9>65<=>591";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * AT = 2
		 */
		String seq2s1 = "@cluster_8:UMI_CTTTGA";
		String seq2s2 = "TATCCUNGCAATATTCTCCGAACNGGAGAG";
		String seq2s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * AT = 3
		 */
		String seq3s1 = "@cluster_12:UMI_GGTCAA";
		String seq3s2 = "GCAGTTNNAGTTATATATTTNNNAGAGCA";
		String seq3s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		SingleFilter filter = new NoPattern();

		ExecutionParametersManager.setParameter("NoPattern", "");
		ExecutionParametersManager.setParameter("RepNoPattern", "");
		filtered = filter.validate(original);
		assertEquals(original.collect(), filtered.collect());

		ExecutionParametersManager.setParameter("NoPattern", "AT");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 3);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 3);
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));

		ExecutionParametersManager.setParameter("NoPattern", "");
		ExecutionParametersManager.setParameter("RepNoPattern", "3");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 3);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 3);
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));

		ExecutionParametersManager.setParameter("NoPattern", "AT");
		ExecutionParametersManager.setParameter("RepNoPattern", "2");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq1));

		ExecutionParametersManager.setParameter("NoPattern", "AT");
		ExecutionParametersManager.setParameter("RepNoPattern", "3");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));
	}
}
