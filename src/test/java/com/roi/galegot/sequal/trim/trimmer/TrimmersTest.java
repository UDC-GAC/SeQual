package com.roi.galegot.sequal.trim.trimmer;

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
import com.roi.galegot.sequal.trimmer.TrimNRight;
import com.roi.galegot.sequal.trimmer.Trimmer;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class TrimmersTest {
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
	public void trimLeft() {

	}

	@Test
	public void trimRight() {

	}

	@Test
	public void trimLeftP() {

	}

	@Test
	public void trimRightP() {

	}

	@Test
	public void trimQualLeft() {

	}

	@Test
	public void trimQualRight() {

	}

	@Test
	public void trimLeftToLength() {

	}

	@Test
	public void trimRightToLength() {

	}

	@Test
	public void trimNRight() {
		/*
		 * Length = 30
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGNNNNN";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1fa = ">cluster_8:UMI_CTTTGA";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2);

		// Copy of above sequence with 25 characters
		String seq4s2 = "TATCCUNGCAATANTCTCCGAACNGN";
		String seq4s4 = "1/04.72,(003,-2-22+00-12./";
		Sequence seq4 = new Sequence(seq1s1, seq4s2, commLine, seq4s4);
		Sequence seq4fa = new Sequence(seq1s1fa, seq4s2);

		/*
		 * Length = 29
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGNNNN";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1fa = ">cluster_12:UMI_GGTCAA";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2);

		// Copy of above sequence with 25 characters
		String seq5s2 = "GCAGTTNNAGATCAATATATNNNAG";
		String seq5s4 = "?7?AEEC@>=1?A?EEEB9ECB?==";
		Sequence seq5 = new Sequence(seq1s1, seq5s2, commLine, seq5s4);
		Sequence seq5fa = new Sequence(seq2s1fa, seq5s2);

		/*
		 * Length = 24
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCANNN";
		String seq3s4 = ">=2.660/?:36AD;0<1470364";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1fa = ">cluster_21:UMI_AGAACA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> trimmered;
		ArrayList<Sequence> list;
		Trimmer trimmer = new TrimNRight();

		// Test for empty RDD
		trimmered = trimmer.trim(emptyRdd);
		assertEquals(0, trimmered.count());

		ExecutionParametersManager.setParameter("TrimNRight", "");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimNRight", "0");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimNRight", "-1");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimNRight", "4");
		trimmered = trimmer.trim(original);
		assertEquals(3, trimmered.count());
		list = new ArrayList<>(trimmered.collect());
		assertEquals(3, list.size());
		assertTrue(list.contains(seq3));
		assertTrue(list.contains(seq4));
		assertTrue(list.contains(seq5));

		trimmered = trimmer.trim(originalFA);
		assertEquals(3, trimmered.count());
		list = new ArrayList<>(trimmered.collect());
		assertEquals(3, list.size());
		assertTrue(list.contains(seq3fa));
		assertTrue(list.contains(seq4fa));
		assertTrue(list.contains(seq5fa));

	}
}