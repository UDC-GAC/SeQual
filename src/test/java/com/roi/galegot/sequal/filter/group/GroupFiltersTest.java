package com.roi.galegot.sequal.filter.group;

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

public class GroupFiltersTest {
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
	public void filterReverseComplementDistinct() {
		String seq1s1 = "@Original";
		String seq1s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq2s1 = "@ComplementReverseCalidadIgual";
		String seq2s2 = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		String seq2s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq3s1 = "@ComplementReverseMasCalidad";
		String seq3s2 = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		String seq3s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq1s1fa = ">Original";
		String seq1s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2fa);

		String seq2s1fa = ">ComplementReverse";
		String seq2s2fa = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2fa);

		String seq3s1fa = ">ComplementReverseCopia";
		String seq3s2fa = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2fa);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		GroupFilter filter = new ReverseComplementDistinct();

		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq3));

		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
	}

}