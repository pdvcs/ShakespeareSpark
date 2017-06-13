package net.pdutta.sparkdemo.ShakespeareAnalytics;

import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

public class Main {
    public static void main (String[] args) {

        if (args.length != 2) {
            System.out.println("Usage: ShakespeareAnalytics /path/to/input-file top-N-above-5-letters");
            System.out.println(" e.g.: ShakespeareAnalytics /home/pd/ws.txt 50");
            System.exit(1);
        }
        String inputFile = args[0];
        File f = new File(inputFile);
        if (!f.exists() || f.isDirectory()) {
            System.out.println("error: could not read: " + inputFile);
            System.exit(2);
        }
        String topNStr = args[1];
        Integer topN = 100;
        try {
            topN = Integer.parseInt(topNStr);
        } catch (NumberFormatException ex) {
            System.out.println("error: please enter a number between 10 and 20000; you entered: " + topNStr);
            System.exit(3);
        }

        Map<String, Integer> wordsWithCounts = analyzeInput(inputFile, topN);
        System.out.println("\n");
        System.out.println(new Gson().toJson(wordsWithCounts));
        System.out.println("\n");
    }

    private static Map<String, Integer> analyzeInput (String inputFile, Integer topN) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ShakespeareAnalytics");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(inputFile);
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("[^A-Za-z']")).iterator())
                .filter(s -> s.trim().length() > 5)
                .mapToPair(word -> new Tuple2<>(normalize(word), 1))
                .reduceByKey((a, b) -> a + b);

        System.out.println("\n*** Total words with length > 5: " + counts.count() + "\n");
        // XX counts.saveAsTextFile("D:/Storage/Downloads/shakespeareWordCount");

        System.out.println("\n*** Top " + topN + " words in Shakespeare with length > 5:\n");
        Map<String, Integer> itemMap = new LinkedHashMap<>();
        for (Tuple2<String, Integer> tuple : counts.takeOrdered(topN, TupleTopNComparator.INSTANCE)) {
            System.out.println("\t" + tuple._1 + ": " + tuple._2);
            itemMap.put(tuple._1, tuple._2);
        }
        return itemMap;
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
        input = input.endsWith("'") ? input.substring(0, input.length() - 1) : input;
        return input.toLowerCase();
    }

    public static class TupleTopNComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        final static TupleTopNComparator INSTANCE = new TupleTopNComparator();

        @Override
        public int compare (Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return -t1._2.compareTo(t2._2); // we need descending-order for top-N
        }
    }


}
