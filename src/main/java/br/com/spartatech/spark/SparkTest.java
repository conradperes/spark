package br.com.spartatech.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTest {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SparkConrad").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		

		List<String>lista= Arrays.asList("Diogo", "Ana", "Pedro", "Fabricio", "Conrad");
		
		JavaRDD<String> rdd = context.parallelize(lista);
		rdd.foreach((nome)-> System.out.println(nome));
	}

}
