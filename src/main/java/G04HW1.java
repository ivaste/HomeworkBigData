import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
        //System.out.print("K= "+K);

        // Read input file
        JavaRDD<String> docs = sc.textFile(args[1]).repartition(K);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        JavaPairRDD<String, Long> output1;
        JavaPairRDD<String, Long> output2;

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CLASS COUNT with DETERMINISTIC PARTITIONS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        output1=docs
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
                .flatMapToPair((pair) -> {
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    String cls=pair._1;
                    long sum = 0;
                    for (long c : pair._2) {
                        sum += c;
                    }
                    pairs.add(new Tuple2<>(cls,sum));
                    
                    return pairs.iterator();
                });

        // Print results
        // TODO ......................................................
        System.out.println("VERSION WITH DETERMINISTIC PARTITIONS");
        System.out.print("Output pairs =");
        //List<> pairs = output.collect();
        for (Tuple2<String,Long> pair: output1.sortByKey().collect()) {
            System.out.print(" ("+pair._1+", "+pair._2+")");
        }
        System.out.println();


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CLASS COUNT with SPARK PARTITIONS
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // TODO.....

    }

}
