package br.com.spartatech.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkWordCount {

	public static void main(String[] args) {
//		wordsCountJava7();
		wordsCountJava8();
	}

	private static void wordsCountJava7() {
		SparkConf conf = new SparkConf().setAppName("SparkWordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> input = context.textFile("/Users/conradmarquesperes/Documents/Shaekspeare.txt");
		JavaRDD<String> palavras = input.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		JavaPairRDD<String, Integer>cont = palavras.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				return new Tuple2<String, Integer>(arg0, 1);
			}
		});
		JavaPairRDD<String, Integer>reduceCount = cont.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return  a+b;
			}
		});
		reduceCount.saveAsTextFile("/Users/conradmarquesperes/Documents/ShaekspeareOutput.txt");
	}

	private static void wordsCountJava8() {
		SparkConf conf = new SparkConf().setAppName("SparkWordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> input = context.textFile("/Users/conradmarquesperes/Documents/Shaekspeare.txt");
		JavaRDD<String> palavras = input.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
				
		JavaPairRDD<String, Integer>cont = palavras.mapToPair(p -> new Tuple2(p,1)).reduceByKey((v1,v2)->(int)v1+(int)v2);
		
		cont.saveAsTextFile("/Users/conradmarquesperes/Documents/ShaekspeareOutput2.txt");
		
	}
}
