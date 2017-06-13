package net.pdutta.sparkdemo.ShakespeareAnalytics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

public class Main {
    public static void main (String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("D:/Storage/Downloads/shakespeare.txt");
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("[^A-Za-z']")).iterator())
                .filter(s -> s.trim().length() > 5)
                .mapToPair(word -> new Tuple2<>(normalize(word), 1))
                .reduceByKey((a, b) -> a + b);

        System.out.println("\n\n*** Total words with length > 5: " + counts.count() + "\n");
        counts.saveAsTextFile("D:/Storage/Downloads/shakespeareWordCount");
        System.out.println("\n\nTop 20 Words in Shakespeare (with length > 5):\n");
        for (Tuple2<String, Integer> tpl : counts.takeOrdered(100, TupleTopNComparator.INSTANCE)) {
            System.out.println(tpl._1 + ": " + tpl._2);
        }
    }

    public static class TupleTopNComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        final static TupleTopNComparator INSTANCE = new TupleTopNComparator();
        @Override
        public int compare (Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return -t1._2.compareTo(t2._2); // we need descending-order for top-N
        }
    }

    /*
     * As we've mostly removed all the other punctuation,
     * we mainly have to do the following:
     *    - remove single quotes at the beginning/end of strings
     *      (single quotes in the middle are okay due to words like "punish'd")
     *    - reduce everything to lowercase to collapse case differences
     */
    private static String normalize (String input) {
        input = input.startsWith("'") ? input.substring(1) : input;
        input =  input.endsWith("'") ? input.substring(0, input.length() - 1) : input;
        return input.toLowerCase();
    }
}
