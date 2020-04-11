import mx4j.log.Log;
import org.apache.hadoop.mapred.lib.LazyOutputFormat;
import org.apache.hadoop.util.hash.Hash;
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

    public static String MAX_PARTITION = "maxPartitionSize";

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
                })
                .sortByKey();
        System.out.println("VERSION WITH DETERMINISTIC PARTITIONS");
        System.out.print("Output pairs = ");

        output1.foreach(data -> {
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
                    return pairs.iterator();
                })
                .mapPartitionsToPair((it) -> {
                    long num = 0;
                    HashMap<String, Long> count = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    while (it.hasNext()) {
                        num++;
                        Tuple2<Long, String> pair = it.next();
                        count.put(pair._2, 1L + count.getOrDefault(pair._2, 0L));
                    }
                    for (Map.Entry<String, Long> entry : count.entrySet()) {
                        pairs.add(new Tuple2<>(entry.getKey(), entry.getValue()));
                    }
                    pairs.add(new Tuple2<>(MAX_PARTITION, num));
                    return pairs.iterator();
                })
                .groupByKey()
                .flatMapToPair((it) -> {
                    long N_max = 0;
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    if (it._1.equals(MAX_PARTITION)) {
                        for (Long n: it._2) {
                            if (n > N_max) {
                                N_max = n;
                            }
                        }
                        pairs.add(new Tuple2<>(MAX_PARTITION, N_max));
                    } else {
                        String cls = it._1;
                        long sum = 0;
                        for (Long n: it._2) {
                            sum += n;
                        }
                        pairs.add(new Tuple2<>(cls, sum));
                    }
                    return pairs.iterator();
                });

        System.out.println("VERSION WITH SPARK PARTITIONS");
        String mfcN="";  //Most frequent class (Name)
        long mfcF=-1;   //Most frequent class (Frequency)
        long n_max=0;
        for (Tuple2<String,Long> pair: output2.collect()) {
            if(pair._1.equals(MAX_PARTITION)){
                n_max=pair._2;
            }
            else{
                if(pair._2>mfcF){
                    mfcN=pair._1;
                    mfcF=pair._2;
                }
            }

        }
        System.out.print("Most frequent class = ");
        System.out.print("("+mfcN+", "+mfcF+")");
        System.out.print("\nMax partition size = "+n_max);
    }

}