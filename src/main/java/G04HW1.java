import mx4j.log.Log;
import org.apache.hadoop.mapred.lib.LazyOutputFormat;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.convert.Wrappers;
import scala.collection.immutable.ListSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class G04HW1 {

    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: number_partitions, <path to file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 2) {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setAppName("Homework1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions
        int K = Integer.parseInt(args[0]);

        // Read input file and subdivide it into K random partitions
        JavaRDD<String> docs = sc.textFile(args[1]).repartition(K);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long numdocs = docs.count();
        System.out.println("Number of documents = " + numdocs);
        JavaPairRDD<String, Long> output1;

        output1 = docs
                .flatMapToPair((document) -> {    // <-- MAP PHASE (R1)
                    String[] tokens = document.split(" ");
                    ArrayList<Tuple2<Long, String>> pairs = new ArrayList<>();
                    long index = Long.parseLong(tokens[0]);
                    String cls = tokens[1];
                    pairs.add(new Tuple2<>(index % K, cls));
                    return pairs.iterator();
                })
                .groupByKey() // <-- REDUCE PHASE (R1)
                .flatMapToPair((tuple) -> {
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String cls: tuple._2) {
                        counts.put(cls, 1L + counts.getOrDefault(cls, 0L));
                    }
                    for (Map.Entry<String, Long> e: counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                .groupByKey() // <-- REDUCE PHASE (R2)
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });
        System.out.println("VERSION WITH DETERMINISTIC PARTITIONS");
        System.out.print("Output pairs = ");

        ArrayList<Tuple2<String, Long>> sorted = new ArrayList<>(output1.collect());
        sorted.sort((x, y) -> x._1.compareTo(y._1));
        sorted.forEach(data -> {
            System.out.print("(" + data._1 + ", " + data._2 + ") ");
        });
        System.out.println();

        JavaPairRDD<String, Long> output2;

        output2 = docs
                .flatMapToPair((document) -> {    // <-- MAP PHASE (R1)
                    String[] tokens = document.split(" ");
                    ArrayList<Tuple2<Long, String>> pairs = new ArrayList<>();
                    long index = Long.parseLong(tokens[0]);
                    String cls = tokens[1];
                    pairs.add(new Tuple2<>(index % K, cls));
                    System.out.println("MAP PHASE 1");
                    return pairs.iterator();
                })
                .mapPartitionsToPair((it) -> {
                    System.out.println(it);
                    while (it.hasNext()) {
                        Tuple2<Long, String> pair = it.next();
                    }
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    return pairs.iterator();
                });
    }

}