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
        // Parameters are: diversity_maxim, number_partitions, <path to file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: diversity_maxim num_partitions file_path");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setAppName("Homework3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions
        int K = Integer.parseInt(args[0]);
        int L = Integer.parseInt(args[1]);;

        // Read input file and subdivide it into L random partitions

        long initStart = System.currentTimeMillis();
        JavaRDD<Vector> rddPoints = sc.textFile(args[2]).map(G04HW3::strToVector).repartition(L);
        long numPoints=rddPoints.count();
        long initEnd = System.currentTimeMillis();
        long initTime = initEnd - initStart;


        System.out.println("Number of points = " +numPoints);
        System.out.println("k = " +K);
        System.out.println("L = " +L);
        System.out.println("Initialization time = " +initTime+ " ms");

        //Round 1
        long round1Start = System.currentTimeMillis();
        JavaRDD<Vector> coresetRdd = runRound1(rddPoints, K, L).cache();
        coresetRdd.count();
        long round1Delta = System.currentTimeMillis() - round1Start;
        System.out.println("Runtime of Round 1 = " +round1Delta+ " ms");


        //ArrayList<Vector> solution = runMapReduce(rddPoints, K, L);
        //Round 2
        long round2Start = System.currentTimeMillis();
        ArrayList<Vector> coreset = new ArrayList<>(coresetRdd.collect());
        ArrayList<Vector> solution = runSequential(coreset, K);
        long round2Delta = System.currentTimeMillis() - round2Start;
        System.out.println("Runtime of Round 2 = " +round2Delta+ " ms");

        System.out.println("Average distance = " +measure(solution));

    }


    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // METHOD runMapReduce
    //
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static JavaRDD<Vector> runRound1(JavaRDD<Vector> points, int K, int L) {
        return points
                //.repartition(L) //<-- Map Phase (R1)
                .mapPartitions( (iterator) -> { //<-- Reduce phase (R1)
                    ArrayList<Vector> vectors = new ArrayList<>();
                    iterator.forEachRemaining(vectors::add);
                    ArrayList<Vector> centers = kCenterMPD(vectors, K); //Farthest-First Traversal algorithm
                    return centers.iterator();
                });
    }

    public static ArrayList<Vector> runMapReduce(JavaRDD<Vector> points, int K, int L) { //Parameter L is used when repartition is called inside this function
        long round1Start = System.currentTimeMillis();

        JavaRDD<Vector> coresetRRD = points
                //.repartition(L) //<-- Map Phase (R1)
                .mapPartitions( (iterator) -> { //<-- Reduce phase (R1)
                    ArrayList<Vector> vectors = new ArrayList<>();
                    iterator.forEachRemaining(vectors::add);
                    ArrayList<Vector> centers = kCenterMPD(vectors, K); //Farthest-First Traversal algorithm
                    return centers.iterator();
                })
                .cache();
        coresetRRD.count(); //Used to avoid lazy evaluation and let system measure the spark's runtime
        long round1End = System.currentTimeMillis();
        long round1Time = round1End - round1Start;
        System.out.println("Runtime of Round 1 = " +round1Time+ " ms");

        //Reduce Phase (R2)
        long round2Start = System.currentTimeMillis();
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
    private static ArrayList<Vector> kCenterMPD(ArrayList<Vector> P, int k) {
        ArrayList<Vector> C = new ArrayList<>();
        ArrayList<Double> dist = new ArrayList<>();             // arraylist of the distances. Simmetrical to P
        for (int i = 0; i < P.size(); i++) {
            dist.add(Double.POSITIVE_INFINITY);                 // initialize all the distances to infinity
        }

        C.add(P.get(0));                                        // choose the first center as a random point of P

        for (int i = 0; i < dist.size(); i++) {
            dist.set(i, Math.sqrt(Vectors.sqdist(C.get(0), P.get(i))));     // Update the distances from the first selected point
        }

        for (int l = 1; l < k; l++) {   //since we select all the k center do:
            int max_index = dist.indexOf(Collections.max(dist)); //choose the point at max distance from the center set
            C.add(P.get(max_index));                             //add to the center set

            //update the disctances in the following way: For each remaining not-yet-selected point q,
            // replace the distance stored for q by the minimum of its old value and the distance from p to q.
            for (int i = 0; i < P.size(); i++) {
                double d1 = dist.get(i);
                double d2 = Math.sqrt(Vectors.sqdist(C.get(l), P.get(i)));
                dist.set(i, Math.min(d1, d2));
            }

        }
        return C;
    }

    public static Vector strToVector(String str) {
        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }


    static double measure(ArrayList<Vector> pointSet) {

        double sum = 0;
        double den=0;
        for (int i = 0; i < pointSet.size(); ++i) {
            Vector p1 = pointSet.get(i);
            double pointDist = 0;
            for (int j = 0; j < pointSet.size(); ++j) {
                Vector p2 = pointSet.get(j);
                if (p1 == p2) continue;
                double dist = Vectors.sqdist(p1, p2);
                dist = Math.sqrt(dist);
                sum+=dist;
                den++;
            }
        }

        return sum/den;
    }

}