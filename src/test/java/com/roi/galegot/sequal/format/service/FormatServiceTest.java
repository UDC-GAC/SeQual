package com.roi.galegot.sequal.format.service;

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
import com.roi.galegot.sequal.service.FormatService;
import com.roi.galegot.sequal.util.ExecutionParametersManager;

public class FormatServiceTest {
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
	public void formatService() {

		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCTCGCAATACTCTCCGAACAGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTGCAGATCAATATATGCTAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq3s1 = ">cluster_8:UMI_CTTTGA";
		String seq3s2 = "UAUCCUCGCAAUACUCUCCGAACAGGAGAG";
		Sequence seq3 = new Sequence(seq3s1, seq3s2);

		String seq4s1 = ">cluster_12:UMI_GGTCAA";
		String seq4s2 = "GCAGUUGCAGAUCAAUAUAUGCUAGAGCA";
		Sequence seq4 = new Sequence(seq4s1, seq4s2);

		Sequence seq5 = new Sequence(seq1s1, seq3s2, commLine, seq1s4);
		Sequence seq6 = new Sequence(seq2s1, seq4s2, commLine, seq2s4);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2));
		JavaRDD<Sequence> formatted;
		ArrayList<Sequence> list;

		ExecutionParametersManager.setParameter("Formatters", "");

		formatted = FormatService.format(original);
		assertEquals(formatted.count(), 2);
		list = new ArrayList<>(formatted.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));

		ExecutionParametersManager.setParameter("Formatters", "DNATORNA");

		formatted = FormatService.format(original);
		assertEquals(formatted.count(), 2);
		list = new ArrayList<>(formatted.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq5));
		assertTrue(list.contains(seq6));

		ExecutionParametersManager.setParameter("Formatters", "DNATORNA|FASTQTOFASTA");

		formatted = FormatService.format(original);
		assertEquals(formatted.count(), 2);
		list = new ArrayList<>(formatted.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq3));
		assertTrue(list.contains(seq4));

	}
}
