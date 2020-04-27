import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class G04HW2 {

    public static void main(String[] args) throws FileNotFoundException {

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

        SparkConf conf = new SparkConf(true).setAppName("Homework2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions
        int K = Integer.parseInt(args[0]);

        // Read input file
        ArrayList<Vector> inputPoints;
        try {
            inputPoints = readVectorsSeq(args[1]);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        exactMPD(inputPoints);


        twoApproxMPD(inputPoints, K);


        kCenterMPD(inputPoints, K);


    }

    /*
     * ===============================================
     *              HOMEWORK METHODS
     * ===============================================
     */
    public static void exactMPD(ArrayList<Vector> inputPoints) {
        double maxDistance = 0;
        long startMs = System.currentTimeMillis();
        //Write code here
        for(Vector p1 : inputPoints) {
            for (Vector p2 : inputPoints) {
                double distance = Vectors.sqdist(p1, p2);
                if (distance > maxDistance) {
                    maxDistance = distance;
                }
            }
        }

        long endMs = System.currentTimeMillis();
        long deltaMs = endMs - startMs;
        System.out.println("EXACT ALGORITHM");
        System.out.println("Max distance = " +maxDistance);
        System.out.println("Running time = " +deltaMs);
        System.out.println();
    }

    public static void twoApproxMPD(ArrayList<Vector> inputPoints, int K) {
        double maxDistance = 0;
        long startMs = System.currentTimeMillis();
        //Write code here


        long endMs = System.currentTimeMillis();
        long deltaMs = endMs - startMs;
        System.out.println("2-APPROXIMATION ALGORITHM");
        System.out.println("k = " +K);
        System.out.println("Max distance = " +maxDistance);
        System.out.println("Running time = " +deltaMs);
        System.out.println();
    }

    public static void kCenterMPD(ArrayList<Vector> inputPoints, int K) {
        double maxDistance = 0;
        long startMs = System.currentTimeMillis();
        //Write code here



        long endMs = System.currentTimeMillis();
        long deltaMs = endMs - startMs;
        System.out.println("k-CENTER-BASED ALGORITHM");
        System.out.println("k = " +K);
        System.out.println("Max distance = " +maxDistance);
        System.out.println("Running time = " +deltaMs);
        System.out.println();
    }

    /*
     * ===============================================
     *              HELPER METHODS
     * ===============================================
     */
    public static Vector strToVector(String str) {
        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

    public static ArrayList<Vector> readVectorsSeq(String filename) throws IOException {
        if (Files.isDirectory(Paths.get(filename))) {
            throw new IllegalArgumentException("readVectorsSeq is meant to read a single file.");
        }
        ArrayList<Vector> result = new ArrayList<>();
        Files.lines(Paths.get(filename))
                .map(str -> strToVector(str))
                .forEach(e -> result.add(e));
        return result;
    }
}
