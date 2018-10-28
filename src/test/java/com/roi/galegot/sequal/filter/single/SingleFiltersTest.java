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
	public void filterLength() {

		/*
		 * Length = 30
		 */
		String seq2s1 = "@cluster_8:UMI_CTTTGA";
		String seq2s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq2s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * Length = 29
		 */
		String seq3s1 = "@cluster_12:UMI_GGTCAA";
		String seq3s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq3s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		/*
		 * Length = 28
		 */
		String seq4s1 = "@cluster_21:UMI_AGAACA";
		String seq4s2 = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq4s4 = ">=2.660/?:36AD;0<14703640334";
		Sequence seq4 = new Sequence(seq4s1, seq4s2, commLine, seq4s4);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq2, seq3, seq4));
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		SingleFilter filter = new Length();

		ExecutionParametersManager.setParameter("LengthMinVal", "");
		ExecutionParametersManager.setParameter("LengthMaxVal", "");
		filtered = filter.validate(original);
		assertEquals(original.collect(), filtered.collect());

		ExecutionParametersManager.setParameter("LengthMinVal", "29");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));

		ExecutionParametersManager.setParameter("LengthMinVal", "");
		ExecutionParametersManager.setParameter("LengthMaxVal", "29");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq3));
		assertTrue(list.contains(seq4));

		ExecutionParametersManager.setParameter("LengthMinVal", "29");
		ExecutionParametersManager.setParameter("LengthMaxVal", "29");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq3));
	}

	@Test
	public void filterQuality() {

		/*
		 * Quality = 25.29032258064516
		 */
		String seq1s1 = "@cluster_2:UMI_ATTCCG";
		String seq1s2 = "TTTCCGGGGCACATAATCTTCAGCCGGGCGC";
		String seq1s4 = "9C;=;=<9@4868>9:67AA<9>65<=>591";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * Quality = 30.103448275862068
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * Quality =20.964285714285715
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq3s4 = ">=2.660/?:36AD;0<14703640334";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		SingleFilter filter = new Quality();

		ExecutionParametersManager.setParameter("QualityMinVal", "");
		ExecutionParametersManager.setParameter("QualityMaxVal", "");
		filtered = filter.validate(original);
		assertEquals(original.collect(), filtered.collect());

		ExecutionParametersManager.setParameter("QualityMinVal", "21");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));

		ExecutionParametersManager.setParameter("QualityMinVal", "");
		ExecutionParametersManager.setParameter("QualityMaxVal", "30");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq3));

		ExecutionParametersManager.setParameter("QualityMinVal", "21");
		ExecutionParametersManager.setParameter("QualityMaxVal", "30");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq1));
	}

	@Test
	public void filterNAmb() {

		/*
		 * NAmb = 0
		 */
		String seq1s1 = "@cluster_2:UMI_ATTCCG";
		String seq1s2 = "TTTCCGGGGCACATAATCTTCAGCCGGGCGC";
		String seq1s4 = "9C;=;=<9@4868>9:67AA<9>65<=>591";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * NAmb = 1
		 */
		String seq2s1 = "@cluster_8:UMI_CTTTGA";
		String seq2s2 = "TATCCUTGCAATANTCTCCGAACGGGAGAG";
		String seq2s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * NAmb = 2
		 */
		String seq3s1 = "@cluster_12:UMI_GGTCAA";
		String seq3s2 = "GCAGTTTTAGATCAATATATANNAGAGCA";
		String seq3s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		SingleFilter filter = new NAmb();

		ExecutionParametersManager.setParameter("NAmbMinVal", "");
		ExecutionParametersManager.setParameter("NAmbMaxVal", "");
		filtered = filter.validate(original);
		assertEquals(original.collect(), filtered.collect());

		ExecutionParametersManager.setParameter("NAmbMinVal", "1");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));

		ExecutionParametersManager.setParameter("NAmbMinVal", "");
		ExecutionParametersManager.setParameter("NAmbMaxVal", "1");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));

		ExecutionParametersManager.setParameter("NAmbMinVal", "1");
		ExecutionParametersManager.setParameter("NAmbMaxVal", "1");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq2));
	}

	@Test
	public void filterNAmbP() {

		/*
		 * NAmbP = 0
		 */
		String seq1s1 = "@cluster_2:UMI_ATTCCG";
		String seq1s2 = "TTTCCGGGGCACATAATCTTCAGCCGGGCGC";
		String seq1s4 = "9C;=;=<9@4868>9:67AA<9>65<=>591";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * NAmbP = 0.1
		 */
		String seq2s1 = "@cluster_8:UMI_CTTTGA";
		String seq2s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq2s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * NAmbP = 0.1724137931034483
		 */
		String seq3s1 = "@cluster_12:UMI_GGTCAA";
		String seq3s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq3s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		SingleFilter filter = new NAmbP();

		ExecutionParametersManager.setParameter("NAmbPMinVal", "");
		ExecutionParametersManager.setParameter("NAmbPMaxVal", "");
		filtered = filter.validate(original);
		assertEquals(original.collect(), filtered.collect());

		ExecutionParametersManager.setParameter("NAmbPMinVal", "0.05");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));

		ExecutionParametersManager.setParameter("NAmbPMinVal", "");
		ExecutionParametersManager.setParameter("NAmbPMaxVal", "0.1");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));

		ExecutionParametersManager.setParameter("NAmbPMinVal", "0.05");
		ExecutionParametersManager.setParameter("NAmbPMaxVal", "0.1");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq2));
	}

	@Test
	public void filterGC() {

		/*
		 * G = 4 C = 8
		 */
		String seq1s1 = "@cluster_21:UMI_AGAACA";
		String seq1s2 = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq1s4 = ">=2.660/?:36AD;0<14703640334";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * G = 6 C = 7
		 */
		String seq2s1 = "@cluster_8:UMI_CTTTGA";
		String seq2s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq2s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * G = 8 C = 6
		 */
		String seq3s1 = "@cluster_12:UMI_GGTCAA";
		String seq3s2 = "GCCGTCNCAGATCAATATATNGGGGAGCA";
		String seq3s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		SingleFilter filter = new GC();

		ExecutionParametersManager.setParameter("GCMinVal", "");
		ExecutionParametersManager.setParameter("GCMaxVal", "");
		filtered = filter.validate(original);
		assertEquals(original.collect(), filtered.collect());

		ExecutionParametersManager.setParameter("GCMinVal", "13");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));

		ExecutionParametersManager.setParameter("GCMinVal", "");
		ExecutionParametersManager.setParameter("GCMaxVal", "13");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));

		ExecutionParametersManager.setParameter("GCMinVal", "13");
		ExecutionParametersManager.setParameter("GCMaxVal", "13");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq2));
	}

	@Test
	public void filterGCP() {

		/*
		 * GCP = 0.42857142857142855
		 */
		String seq1s1 = "@cluster_21:UMI_AGAACA";
		String seq1s2 = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq1s4 = ">=2.660/?:36AD;0<14703640334";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * GCP = 0.43333333333333335
		 */
		String seq2s1 = "@cluster_8:UMI_CTTTGA";
		String seq2s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq2s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * GCP = 0.4444444444444444
		 */
		String seq3s1 = "@cluster_29:UMI_GCAGGA";
		String seq3s2 = "CCCCCTTAAATAGCTGTTTATTTGGCC";
		String seq3s4 = "8;;;>DC@DAC=B?C@9?B?CDCB@><";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		SingleFilter filter = new GCP();

		ExecutionParametersManager.setParameter("GCPMinVal", "");
		ExecutionParametersManager.setParameter("GCPMaxVal", "");
		filtered = filter.validate(original);
		assertEquals(original.collect(), filtered.collect());

		ExecutionParametersManager.setParameter("GCPMinVal", "0.43");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));

		ExecutionParametersManager.setParameter("GCPMinVal", "");
		ExecutionParametersManager.setParameter("GCPMaxVal", "0.44");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));

		ExecutionParametersManager.setParameter("GCPMinVal", "0.43");
		ExecutionParametersManager.setParameter("GCPMaxVal", "0.44");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq2));
	}

	@Test
	public void filterNonIupac() {

		/*
		 * U = 0
		 */
		String seq1s1 = "@cluster_2:UMI_ATTCCG";
		String seq1s2 = "TTTCCGGGGCACATAATCTTCAGCCGGGCGC";
		String seq1s4 = "9C;=;=<9@4868>9:67AA<9>65<=>591";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * U = 1
		 */
		String seq2s1 = "@cluster_8:UMI_CTTTGA";
		String seq2s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq2s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * U = 0
		 */
		String seq3s1 = "@cluster_12:UMI_GGTCAA";
		String seq3s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq3s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		SingleFilter filter = new NonIupac();

		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq3));
	}

	@Test
	public void filterPattern() {

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
		SingleFilter filter = new Pattern();

		ExecutionParametersManager.setParameter("Pattern", "");
		ExecutionParametersManager.setParameter("RepPattern", "");
		filtered = filter.validate(original);
		assertEquals(original.collect(), filtered.collect());

		ExecutionParametersManager.setParameter("Pattern", "AT");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 3);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 3);
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));

		ExecutionParametersManager.setParameter("Pattern", "");
		ExecutionParametersManager.setParameter("RepPattern", "3");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 3);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 3);
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));

		ExecutionParametersManager.setParameter("Pattern", "AT");
		ExecutionParametersManager.setParameter("RepPattern", "2");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));

		ExecutionParametersManager.setParameter("Pattern", "AT");
		ExecutionParametersManager.setParameter("RepPattern", "3");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq3));
	}
}
