package com.refactorlabs.cs378.assign9;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by davidfranke on 11/1/16.
 */
public class WordCountWithLambdas {

    public static void main(String[] args) {
        // Utils.printClassPath();

        String inputFilename = args[0];
        String outputFilename = args[1];

        // Create a Java Spark context
        SparkConf conf = new SparkConf().setAppName(WordCount.class.getName()).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the input data
        JavaRDD<String> input = sc.textFile(inputFilename);

        JavaRDD<String> words = input.flatMap((String s) -> Lists.newArrayList(StringUtils.split(s)).iterator());
        JavaPairRDD<String, Integer> wordsWithCount = words.mapToPair((String s) -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> counts = wordsWithCount.reduceByKey((Integer i1, Integer i2) -> i1 + i2);
        JavaPairRDD<String, Integer> ordered = counts.sortByKey();

        // Save the word count to a text file (initiates evaluation)
        ordered.saveAsTextFile(outputFilename);

        // Shut down the context
        sc.stop();
    }

}
