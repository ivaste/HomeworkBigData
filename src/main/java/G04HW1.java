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

        output1=docs
                .flatMapToPair((document) -> {    // <-- MAP PHASE (R1)

                    // splits the document into tokens using single space as separator
                    String[] tokens = document.split(" ");

                    // creates the ArrayList containing the key-value pairs
                    ArrayList<Tuple2<Long, String>> pairs = new ArrayList<>();

                    // creates index and class from the tokens
                    long index = Long.parseLong(tokens[0]);
                    String cls = tokens[1];

                    // adds a new pair with key (index mod k) and value (cls)
                    pairs.add(new Tuple2<>(index % K, cls));
                    return pairs.iterator();
                })

                .groupByKey() // <-- REDUCE PHASE (R1)

                .flatMapToPair((tuple) -> {

                    // creates a HashMap to keep the number of occurrences for each class
                    HashMap<String, Long> counts = new HashMap<>();

                    // creates the ArrayList containing the new key-value pairs
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                    // adds 1 to the count of the class if the element exists,
                    // otherwise it is created. O(1)
                    for (String cls: tuple._2) {
                        counts.put(cls, 1L + counts.getOrDefault(cls, 0L));
                    }

                    // adds the new key-value pair with class as key and count as value
                    for (Map.Entry<String, Long> e: counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })

                // EMPTY MAP PHASE (R2)

                .groupByKey() // <-- REDUCE PHASE (R2)

                .flatMapToPair((pair) -> {

                    // creates the ArrayList containing the new key-value pairs
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                    // sums all the counts with the same key class
                    String cls=pair._1;
                    long sum = 0;
                    for (long c : pair._2) {
                        sum += c;
                    }
                    pairs.add(new Tuple2<>(cls,sum));

                    return pairs.iterator();
                })

                .sortByKey();

        // Print results
        System.out.println("VERSION WITH DETERMINISTIC PARTITIONS");
        System.out.print("Output pairs =");
        for (Tuple2<String,Long> pair: output1.collect()) {
            System.out.print(" ("+pair._1+", "+pair._2+")");
        }
        System.out.println();


        JavaPairRDD<String, Long> output2;

        output2 = docs
                .flatMapToPair((document) -> {    // <-- MAP PHASE (R1)

                    // splits the document into tokens using single space as separator
                    String[] tokens = document.split(" ");

                    // creates the ArrayList containing the key-value pairs
                    ArrayList<Tuple2<Long, String>> pairs = new ArrayList<>();

                    // creates index and class from the tokens
                    long index = Long.parseLong(tokens[0]);
                    String cls = tokens[1];

                    // adds a new pair with key (index mod k) and value (cls)
                    pairs.add(new Tuple2<>(index % K, cls));
                    return pairs.iterator();
                })

                .mapPartitionsToPair((it) -> {    // <-- REDUCE PHASE (R1)

                    // number of pairs processed
                    long num = 0;

                    // creates a HashMap to keep the number of occurrences for each class
                    HashMap<String, Long> count = new HashMap<>();

                    // creates the ArrayList containing the new key-value pairs
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                    // counts the number of pairs processed (partition size) and
                    // adds 1 to the count of the class if the element exists,
                    // otherwise it is created. O(1)
                    while (it.hasNext()) {
                        num++;
                        Tuple2<Long, String> pair = it.next();
                        count.put(pair._2, 1L + count.getOrDefault(pair._2, 0L));
                    }

                    // adds the new key-value pair with class as key and count as value
                    for (Map.Entry<String, Long> entry : count.entrySet()) {
                        pairs.add(new Tuple2<>(entry.getKey(), entry.getValue()));
                    }

                    // adds the extra key-value pair containing the number of processed pairs (partition size)
                    pairs.add(new Tuple2<>(MAX_PARTITION, num));
                    return pairs.iterator();
                })

                .groupByKey()     // <-- REDUCE PHASE (R2)

                .flatMapToPair((it) -> {

                    // number of max partition size
                    long N_max = 0;

                    // creates the ArrayList containing the new key-value pairs
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                    // if the pair has key MAX_PARTITION, stores in N_max the maximum partition size
                    if (it._1.equals(MAX_PARTITION)) {
                        for (Long n: it._2) {
                            if (n > N_max) {
                                N_max = n;
                            }
                        }
                        // creates the final pair containing the max partition size
                        pairs.add(new Tuple2<>(MAX_PARTITION, N_max));

                        // sums all the counts with the same key class
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
        String mfcN="";  // Most frequent class (Name)
        long mfcF=-1;   // Most frequent class (Frequency)
        long n_max=0;   // Max partition size
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