import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class G04HW2 {

    public static long SEED = 1231829; //Random seed unipd student's id

    public static void main(String[] args) {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: number_partitions, <path to file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 2) {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions
        int K = Integer.parseInt(args[1]);

        // Read input file
        ArrayList<Vector> inputPoints;
        try {
            inputPoints = readVectorsSeq(args[0]);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        long startMs = System.currentTimeMillis();
        double exactDistance = exactMPD(inputPoints);
        long endMs = System.currentTimeMillis();
        long deltaMs = endMs - startMs;
        System.out.println("EXACT ALGORITHM");
        System.out.println("Max distance = " +exactDistance);
        System.out.println("Running time = " +deltaMs+ " ms");
        System.out.println();


        startMs = System.currentTimeMillis();
        double twoApproxDistance = twoApproxMPD(inputPoints, K);
        endMs = System.currentTimeMillis();
        deltaMs = endMs - startMs;
        System.out.println("2-APPROXIMATION ALGORITHM");
        System.out.println("k = " +K);
        System.out.println("Max distance = " +twoApproxDistance);
        System.out.println("Running time = " +deltaMs+ " ms");
        System.out.println();

        startMs = System.currentTimeMillis();
        ArrayList<Vector> centers = kCenterMPD(inputPoints, K);
        double kCenterDistance = exactMPD(centers);
        endMs = System.currentTimeMillis();
        deltaMs = endMs - startMs;
        System.out.println("k-CENTER-BASED ALGORITHM");
        System.out.println("k = " +K);
        System.out.println("Max distance = " +kCenterDistance);
        System.out.println("Running time = " +deltaMs+ " ms");
        System.out.println();


    }

    /*
     * ===============================================
     *              HOMEWORK METHODS
     * ===============================================
     */
    public static double exactMPD(ArrayList<Vector> inputPoints) {
        double maxDistance = 0;
        for(Vector p1 : inputPoints) {
            for (Vector p2 : inputPoints) {
                double distance = Vectors.sqdist(p1, p2);
                if (distance > maxDistance) {
                    maxDistance = distance;
                }
            }
        }
        return Math.sqrt(maxDistance);
    }

    public static double twoApproxMPD(ArrayList<Vector> inputPoints, int K) {
        if (K < 0 || K >= inputPoints.size()) {
            throw new IllegalArgumentException("K should be > 0 and < inputPoints.size()");
        }
        double maxDistance = 0;
        ArrayList<Vector> randomPoints = new ArrayList<>();

        //Choosing K random elements from inputPoints and putting them in randomPoints
        Random random = new Random(SEED);
        //O(K)
        for (int i = 0; i < K; ++i) {
            int index = random.nextInt(inputPoints.size()); //O(1)
            randomPoints.add(inputPoints.get(index)); //O(1)
            inputPoints.remove(index); //Removing elements from inputPoints to avoid checking twice the elements which are in randomPoints
        }

        for (Vector p1 : randomPoints) { //O(K)
            for (Vector p2 : inputPoints) { // O(N - K)
                double distance = Vectors.sqdist(p1, p2);
                if (distance > maxDistance) {
                    maxDistance = distance;
                }
            }
        }
        return Math.sqrt(maxDistance);
    }

    public static ArrayList<Vector> kCenterMPD(ArrayList<Vector> inputPoints, int K) {
        if (K < 0 || K >= inputPoints.size()) {
            throw new IllegalArgumentException("K should be > 0 and < inputPoints.size()");
        }

        ArrayList<Vector> centers = new ArrayList<>(); //Our set S
        Map<Vector, Tuple2<Vector, Double>> mappedPoints = new HashMap<>(); //Stores the point as a key and a tuple as a value which stores the point's center and its distance from the center
        //First random point ck
        Random random = new Random(SEED);
        int index = random.nextInt(inputPoints.size());
        centers.add(inputPoints.get(index));

        inputPoints.remove(index); //P - S
        for (Vector p : inputPoints) {//O(N)
            double dist = Vectors.sqdist(p, centers.get(0));
            mappedPoints.put(p, new Tuple2<>(centers.get(0), dist));
        }

        //The cycle should run in O(K*(N+N)) = O(K*N) where N = inputPoints.size()
        for (int i = 0; i < K - 1; ++i) { //K-1 since the first element is already added. O(K)
            Vector newCenter = null;
            double distance = 0;
            int indexOfNewCenter = -1; //Stores the index od the new center in order to remove it in O(1) from inputPoints

            for (int j = 0; j < inputPoints.size(); ++j) {  //O(N)
                Vector p = inputPoints.get(j); //O(1)
                Tuple2<Vector, Double> tuple = mappedPoints.get(p); //O(1) hashmap!
                //Checks which point is the farthest from its center
                if (tuple._2 > distance) { //O(1)
                    distance = tuple._2;
                    newCenter = p;
                    indexOfNewCenter = j;
                }
            }

            if (newCenter != null) {
                centers.add(newCenter); //O(1)
                inputPoints.remove(indexOfNewCenter); //P - S O(1)

                //Updates the center of the points where the distance of the new added center is less equal than the older center
                for (Vector p : inputPoints) { //O(N)
                    Tuple2<Vector, Double> tuple = mappedPoints.get(p); //O(1) hashmap!
                    double dist = Vectors.sqdist(p, newCenter); //O(1)
                    dist = Math.sqrt(dist);
                    if (dist < tuple._2) { //O(1)
                        mappedPoints.remove(p);
                        mappedPoints.put(p, new Tuple2<>(newCenter, dist)); //O(1) hashmap!
                    }
                }
            }
        }
        assert centers.size() == K; //Makes sure that there are K points

        return centers;
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
