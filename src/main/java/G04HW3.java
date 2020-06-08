import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class G04HW3 {

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
        int L = 5;

        // Read input file and subdivide it into K random partitions

        long initStart = System.currentTimeMillis();
        JavaRDD<Vector> rddPoints = sc.textFile(args[1]).map(G04HW3::strToVector).cache();
        long initEnd = System.currentTimeMillis();
        long initTime = initEnd - initStart;
        System.out.println("Number of points = " +rddPoints.count());
        System.out.println("k = " +K);
        System.out.println("L = " +L);
        System.out.println("Initialization time = " +initTime+ " ms");

        ArrayList<Vector> solution = runMapReduce(rddPoints, K, L);

        System.out.println("Average distance = " +measure(solution));

        System.out.println("Solution: " +solution);
    }

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // METHOD runMapReduce
    //
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static ArrayList<Vector> runMapReduce(JavaRDD<Vector> points, int K, int L) {
        long round1Start = System.currentTimeMillis();

        JavaRDD<Vector> coresetRRD = points
                .repartition(L) //<-- Map Phase (R1)
                .mapPartitions( (iterator) -> { //<-- Reduce phase (R1)
                    ArrayList<Vector> vectors = new ArrayList<>();
                    iterator.forEachRemaining(vectors::add);
                    ArrayList<Vector> centers = kCenterMPD(vectors, K);
                    return centers.iterator();
                 });
        coresetRRD.count(); //Used to avoid lazy evaluation and let system to measure the spark's runtime
        long round1End = System.currentTimeMillis();
        long round1Time = round1End - round1Start;
        System.out.println("Runtime of Round 1 = " +round1Time+ " ms");

        long round2Start = System.currentTimeMillis();
        //Reduce pahse (R2)
        ArrayList<Vector> coreset = new ArrayList<>(coresetRRD.collect());
        ArrayList<Vector> solution = runSequential(coreset, K);

        long round2End = System.currentTimeMillis();
        long round2Time = round2End - round2Start;
        System.out.println("Runtime of Round 2 = " +round2Time+ " ms");

        return solution;
    }

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // METHOD runSequential
    // Sequential 2-approximation based on matching
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static ArrayList<Vector> runSequential(final ArrayList<Vector> points, int k) {

        final int n = points.size();
        if (k >= n) {
            return points;
        }

        ArrayList<Vector> result = new ArrayList<>(k);
        boolean[] candidates = new boolean[n];
        Arrays.fill(candidates, true);
        for (int iter=0; iter<k/2; iter++) {
            // Find the maximum distance pair among the candidates
            double maxDist = 0;
            int maxI = 0;
            int maxJ = 0;
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    for (int j = i+1; j < n; j++) {
                        if (candidates[j]) {
                            // Use squared euclidean distance to avoid an sqrt computation!
                            double d = Vectors.sqdist(points.get(i), points.get(j));
                            if (d > maxDist) {
                                maxDist = d;
                                maxI = i;
                                maxJ = j;
                            }
                        }
                    }
                }
            }
            // Add the points maximizing the distance to the solution
            result.add(points.get(maxI));
            result.add(points.get(maxJ));
            // Remove them from the set of candidates
            candidates[maxI] = false;
            candidates[maxJ] = false;
        }
        // Add an arbitrary point to the solution, if k is odd.
        if (k % 2 != 0) {
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    result.add(points.get(i));
                    break;
                }
            }
        }
        if (result.size() != k) {
            throw new IllegalStateException("Result of the wrong size");
        }
        return result;

    } // END runSequential

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // METHOD kCenterMPD
    // Sequential farthest first traversal algorighm
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static ArrayList<org.apache.spark.mllib.linalg.Vector> kCenterMPD(ArrayList<org.apache.spark.mllib.linalg.Vector> inputPoints, int K) {
        if (K < 0 || K >= inputPoints.size()) {
            throw new IllegalArgumentException("K should be > 0 and < inputPoints.size()");
        }

        long SEED = 1231829; //Here you can change the seed

        ArrayList<org.apache.spark.mllib.linalg.Vector> centers = new ArrayList<>(); //Our set S
        Map<org.apache.spark.mllib.linalg.Vector, Tuple2<org.apache.spark.mllib.linalg.Vector, Double>> mappedPoints = new HashMap<>(); //Stores the point as a key and a tuple as a value which stores the point's center and its distance from the center
        //First random point ck
        Random random = new Random(SEED);
        int index = random.nextInt(inputPoints.size());
        centers.add(inputPoints.get(index));

        inputPoints.remove(index); //P - S
        for (org.apache.spark.mllib.linalg.Vector p : inputPoints) {//O(N)
            double dist = org.apache.spark.mllib.linalg.Vectors.sqdist(p, centers.get(0));
            mappedPoints.put(p, new Tuple2<>(centers.get(0), dist));
        }

        //The cycle should run in O(K*(N+N)) = O(K*N) where N = inputPoints.size()
        for (int i = 0; i < K - 1; ++i) { //K-1 since the first element is already added. O(K)
            org.apache.spark.mllib.linalg.Vector newCenter = null;
            double distance = 0;
            int indexOfNewCenter = -1; //Stores the index od the new center in order to remove it in O(1) from inputPoints

            for (int j = 0; j < inputPoints.size(); ++j) {  //O(N)
                org.apache.spark.mllib.linalg.Vector p = inputPoints.get(j); //O(1)
                Tuple2<org.apache.spark.mllib.linalg.Vector, Double> tuple = mappedPoints.get(p); //O(1) hashmap!
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
                for (org.apache.spark.mllib.linalg.Vector p : inputPoints) { //O(N)
                    Tuple2<org.apache.spark.mllib.linalg.Vector, Double> tuple = mappedPoints.get(p); //O(1) hashmap!
                    double dist = org.apache.spark.mllib.linalg.Vectors.sqdist(p, newCenter); //O(1)
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

    public static Vector strToVector(String str) {
        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

    static double measure(ArrayList<Vector> pointSet) { //Completely wrong. Do it again from scratch
        double sum = 0;
        for (int i = 0; i < pointSet.size(); ++i) {
            Vector p1 = pointSet.get(i);
            double pointDist = 0;
            for (int j = 0; j < pointSet.size(); ++j) {
                Vector p2 = pointSet.get(j);
                if (p1 == p2) continue;
                double dist = Vectors.sqdist(p1, p2);
                dist = Math.sqrt(dist);
                pointDist += dist;
            }
            pointDist /= pointSet.size() - 1;
            sum += pointDist;
        }
        return sum / pointSet.size();
    }
}
