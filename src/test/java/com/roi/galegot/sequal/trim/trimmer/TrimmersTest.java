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
import com.roi.galegot.sequal.trimmer.TrimLeft;
import com.roi.galegot.sequal.trimmer.TrimLeftP;
import com.roi.galegot.sequal.trimmer.TrimLeftToLength;
import com.roi.galegot.sequal.trimmer.TrimQualLeft;
import com.roi.galegot.sequal.trimmer.TrimRight;
import com.roi.galegot.sequal.trimmer.TrimRightP;
import com.roi.galegot.sequal.trimmer.TrimRightToLength;
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
		/*
		 * Length = 30
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1fa = ">cluster_8:UMI_CTTTGA";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2);

		// Copy of above sequence with 5 characters
		String seq4s2 = "GAGAG";
		String seq4s4 = "/.-.4";
		Sequence seq4 = new Sequence(seq1s1, seq4s2, commLine, seq4s4);
		Sequence seq4fa = new Sequence(seq1s1fa, seq4s2);

		/*
		 * Length = 29
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1fa = ">cluster_12:UMI_GGTCAA";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2);

		// Copy of above sequence with 4 characters
		String seq5s2 = "AGCA";
		String seq5s4 = ":B.A";
		Sequence seq5 = new Sequence(seq1s1, seq5s2, commLine, seq5s4);
		Sequence seq5fa = new Sequence(seq2s1fa, seq5s2);

		/*
		 * Length = 24
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCC";
		String seq3s4 = ">=2.660/?:36AD;0<1470364";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1fa = ">cluster_21:UMI_AGAACA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> trimmered;
		ArrayList<Sequence> list;
		Trimmer trimmer = new TrimLeft();

		// Test for empty RDD
		trimmered = trimmer.trim(emptyRdd);
		assertEquals(0, trimmered.count());

		ExecutionParametersManager.setParameter("TrimLeft", "");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimLeft", "0");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimLeft", "-1");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimLeft", "25");
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

	@Test
	public void trimRight() {
		/*
		 * Length = 30
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1fa = ">cluster_8:UMI_CTTTGA";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2);

		// Copy of above sequence with 5 characters
		String seq4s2 = "TATCC";
		String seq4s4 = "1/04.";
		Sequence seq4 = new Sequence(seq1s1, seq4s2, commLine, seq4s4);
		Sequence seq4fa = new Sequence(seq1s1fa, seq4s2);

		/*
		 * Length = 29
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1fa = ">cluster_12:UMI_GGTCAA";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2);

		// Copy of above sequence with 4 characters
		String seq5s2 = "GCAG";
		String seq5s4 = "?7?A";
		Sequence seq5 = new Sequence(seq1s1, seq5s2, commLine, seq5s4);
		Sequence seq5fa = new Sequence(seq2s1fa, seq5s2);

		/*
		 * Length = 24
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCC";
		String seq3s4 = ">=2.660/?:36AD;0<1470364";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1fa = ">cluster_21:UMI_AGAACA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> trimmered;
		ArrayList<Sequence> list;
		Trimmer trimmer = new TrimRight();

		// Test for empty RDD
		trimmered = trimmer.trim(emptyRdd);
		assertEquals(0, trimmered.count());

		ExecutionParametersManager.setParameter("TrimRight", "");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimRight", "0");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimRight", "-1");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimRight", "25");
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

	@Test
	public void trimLeftP() {
		/*
		 * Length = 30
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1fa = ">cluster_8:UMI_CTTTGA";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2);

		// Copy of above sequence with 27 characters
		String seq4s2 = "CCUNGCAATANTCTCCGAACNGGAGAG";
		String seq4s4 = "4.72,(003,-2-22+00-12./.-.4";
		Sequence seq4 = new Sequence(seq1s1, seq4s2, commLine, seq4s4);
		Sequence seq4fa = new Sequence(seq1s1fa, seq4s2);

		/*
		 * Length = 29
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1fa = ">cluster_12:UMI_GGTCAA";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2);

		// Copy of above sequence with 27 characters
		String seq5s2 = "AGTTNNAGATCAATATATNNNAGAGCA";
		String seq5s4 = "?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq5 = new Sequence(seq1s1, seq5s2, commLine, seq5s4);
		Sequence seq5fa = new Sequence(seq2s1fa, seq5s2);

		/*
		 * Length = 24
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCC";
		String seq3s4 = ">=2.660/?:36AD;0<1470364";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1fa = ">cluster_21:UMI_AGAACA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2);

		// Copy of above sequence with 22 characters
		String seq6s2 = "CATTGCAAAATTTNTTSCACCC";
		String seq6s4 = "2.660/?:36AD;0<1470364";
		Sequence seq6 = new Sequence(seq1s1, seq6s2, commLine, seq6s4);
		Sequence seq6fa = new Sequence(seq2s1fa, seq6s2);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> trimmered;
		ArrayList<Sequence> list;
		Trimmer trimmer = new TrimLeftP();

		// Test for empty RDD
		trimmered = trimmer.trim(emptyRdd);
		assertEquals(0, trimmered.count());

		ExecutionParametersManager.setParameter("TrimLeftP", "");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimLeftP", "0");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimLeftP", "-1");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimLeftP", "0.10");
		trimmered = trimmer.trim(original);
		assertEquals(3, trimmered.count());
		list = new ArrayList<>(trimmered.collect());
		assertEquals(3, list.size());

		assertTrue(list.contains(seq4));
		assertTrue(list.contains(seq5));
		assertTrue(list.contains(seq6));

		trimmered = trimmer.trim(originalFA);
		assertEquals(3, trimmered.count());
		list = new ArrayList<>(trimmered.collect());
		assertEquals(3, list.size());
		assertTrue(list.contains(seq4fa));
		assertTrue(list.contains(seq5fa));
		assertTrue(list.contains(seq6fa));
	}

	@Test
	public void trimRightP() {
		/*
		 * Length = 30
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1fa = ">cluster_8:UMI_CTTTGA";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2);

		// Copy of above sequence with 27 characters
		String seq4s2 = "TATCCUNGCAATANTCTCCGAACNGGA";
		String seq4s4 = "1/04.72,(003,-2-22+00-12./.";
		Sequence seq4 = new Sequence(seq1s1, seq4s2, commLine, seq4s4);
		Sequence seq4fa = new Sequence(seq1s1fa, seq4s2);

		/*
		 * Length = 29
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1fa = ">cluster_12:UMI_GGTCAA";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2);

		// Copy of above sequence with 27 characters
		String seq5s2 = "GCAGTTNNAGATCAATATATNNNAGAG";
		String seq5s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B";
		Sequence seq5 = new Sequence(seq1s1, seq5s2, commLine, seq5s4);
		Sequence seq5fa = new Sequence(seq2s1fa, seq5s2);

		/*
		 * Length = 24
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCC";
		String seq3s4 = ">=2.660/?:36AD;0<1470364";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1fa = ">cluster_21:UMI_AGAACA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2);

		// Copy of above sequence with 22 characters
		String seq6s2 = "GGCATTGCAAAATTTNTTSCAC";
		String seq6s4 = ">=2.660/?:36AD;0<14703";
		Sequence seq6 = new Sequence(seq1s1, seq6s2, commLine, seq6s4);
		Sequence seq6fa = new Sequence(seq2s1fa, seq6s2);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> trimmered;
		ArrayList<Sequence> list;
		Trimmer trimmer = new TrimRightP();

		// Test for empty RDD
		trimmered = trimmer.trim(emptyRdd);
		assertEquals(0, trimmered.count());

		ExecutionParametersManager.setParameter("TrimRightP", "");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimRightP", "0");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimRightP", "-1");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimRightP", "0.10");
		trimmered = trimmer.trim(original);
		assertEquals(3, trimmered.count());
		list = new ArrayList<>(trimmered.collect());
		assertEquals(3, list.size());

		assertTrue(list.contains(seq4));
		assertTrue(list.contains(seq5));
		assertTrue(list.contains(seq6));

		trimmered = trimmer.trim(originalFA);
		assertEquals(3, trimmered.count());
		list = new ArrayList<>(trimmered.collect());
		assertEquals(3, list.size());
		assertTrue(list.contains(seq4fa));
		assertTrue(list.contains(seq5fa));
		assertTrue(list.contains(seq6fa));

	}

	@Test
	public void trimQualLeft() {
		/*
		 * Length = 30 Quality = 14,566666
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1fa = ">cluster_8:UMI_CTTTGA";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2);

		// Copy of above sequence with 1 character
		String seq7s2 = "G";
		String seq7s4 = "4";
		Sequence seq7 = new Sequence(seq1s1, seq7s2, commLine, seq7s4);

		/*
		 * Length = 29 Quality = 30.103448275862068
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1fa = ">cluster_12:UMI_GGTCAA";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2);

		// Copy of above sequence with 1 character and quality = 32
		String seq8s2 = "A";
		String seq8s4 = "A";
		Sequence seq8 = new Sequence(seq2s1, seq8s2, commLine, seq8s4);

		/*
		 * Length = 24 Quality = 21,541666666
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCC";
		String seq3s4 = ">=2.660/?:36AD;0<1470364";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1fa = ">cluster_21:UMI_AGAACA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2);

		// Copy of above sequence with 25 characters and quality = 19,8
		String seq5s2 = "TNTTSCACCC";
		String seq5s4 = ";0<1470364";
		Sequence seq5 = new Sequence(seq1s1, seq5s2, commLine, seq5s4);

		// Copy of above sequence with 1 character
		String seq9s2 = "C";
		String seq9s4 = "4";
		Sequence seq9 = new Sequence(seq3s1, seq9s2, commLine, seq9s4);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<>());
		JavaRDD<Sequence> trimmered;
		ArrayList<Sequence> list;
		Trimmer trimmer = new TrimQualLeft();

		// Test for empty RDD
		trimmered = trimmer.trim(emptyRdd);
		assertEquals(0, trimmered.count());

		ExecutionParametersManager.setParameter("TrimQualLeft", "");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimQualLeft", "0");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimQualLeft", "-1");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimQualLeft", "1");
		trimmered = trimmer.trim(original);
		assertEquals(3, trimmered.count());
		list = new ArrayList<>(trimmered.collect());
		assertEquals(3, list.size());
		assertTrue(list.contains(seq7));
		assertTrue(list.contains(seq8));
		assertTrue(list.contains(seq9));

		ExecutionParametersManager.setParameter("TrimQualLeft", "20");
		trimmered = trimmer.trim(original);
		assertEquals(3, trimmered.count());
		list = new ArrayList<>(trimmered.collect());
		assertEquals(3, list.size());
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq5));
		assertTrue(list.contains(seq8));

		trimmered = trimmer.trim(originalFA);
		assertEquals(3, trimmered.count());
		list = new ArrayList<>(trimmered.collect());
		assertEquals(3, list.size());
		assertTrue(list.contains(seq1fa));
		assertTrue(list.contains(seq2fa));
		assertTrue(list.contains(seq3fa));
	}

	@Test
	public void trimQualRight() {

	}

	@Test
	public void trimLeftToLength() {
		/*
		 * Length = 30
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1fa = ">cluster_8:UMI_CTTTGA";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2);

		// Copy of above sequence with 25 characters
		String seq4s2 = "UNGCAATANTCTCCGAACNGGAGAG";
		String seq4s4 = "72,(003,-2-22+00-12./.-.4";
		Sequence seq4 = new Sequence(seq1s1, seq4s2, commLine, seq4s4);
		Sequence seq4fa = new Sequence(seq1s1fa, seq4s2);

		/*
		 * Length = 29
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1fa = ">cluster_12:UMI_GGTCAA";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2);

		// Copy of above sequence with 25 characters
		String seq5s2 = "TTNNAGATCAATATATNNNAGAGCA";
		String seq5s4 = "EEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq5 = new Sequence(seq1s1, seq5s2, commLine, seq5s4);
		Sequence seq5fa = new Sequence(seq2s1fa, seq5s2);

		/*
		 * Length = 24
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCC";
		String seq3s4 = ">=2.660/?:36AD;0<1470364";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1fa = ">cluster_21:UMI_AGAACA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> trimmered;
		ArrayList<Sequence> list;
		Trimmer trimmer = new TrimLeftToLength();

		// Test for empty RDD
		trimmered = trimmer.trim(emptyRdd);
		assertEquals(0, trimmered.count());

		ExecutionParametersManager.setParameter("TrimLeftToLength", "");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimLeftToLength", "0");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimLeftToLength", "-1");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimLeftToLength", "25");
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

	@Test
	public void trimRightToLength() {
		/*
		 * Length = 30
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1fa = ">cluster_8:UMI_CTTTGA";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2);

		// Copy of above sequence with 25 characters
		String seq4s2 = "TATCCUNGCAATANTCTCCGAACNG";
		String seq4s4 = "1/04.72,(003,-2-22+00-12.";
		Sequence seq4 = new Sequence(seq1s1, seq4s2, commLine, seq4s4);
		Sequence seq4fa = new Sequence(seq1s1fa, seq4s2);

		/*
		 * Length = 29
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
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
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCC";
		String seq3s4 = ">=2.660/?:36AD;0<1470364";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1fa = ">cluster_21:UMI_AGAACA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> trimmered;
		ArrayList<Sequence> list;
		Trimmer trimmer = new TrimRightToLength();

		// Test for empty RDD
		trimmered = trimmer.trim(emptyRdd);
		assertEquals(0, trimmered.count());

		ExecutionParametersManager.setParameter("TrimRightToLength", "");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimRightToLength", "0");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimRightToLength", "-1");
		trimmered = trimmer.trim(original);
		assertEquals(original.collect(), trimmered.collect());

		ExecutionParametersManager.setParameter("TrimRightToLength", "25");
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