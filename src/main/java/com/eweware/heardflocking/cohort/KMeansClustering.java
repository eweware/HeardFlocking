package com.eweware.heardflocking.cohort;

import java.util.Arrays;

/**
 * Created by weihan on 10/7/14.
 */
public class KMeansClustering {
    public static int[] run(double[][] data, int numCluster) {
        // N samples, M features
        int N = data.length;
        int M = data[0].length;

        // clustering result
        int[] cluster = new int[N];

        // randomize cluster center
        double[][] centers = new double[numCluster][M];
        for (int c = 0; c < numCluster; c++) {
            int idx = randomIntRange(0, N - 1);
            centers[c] = Arrays.copyOf(data[idx], M);
        }

        // assign every user to a cohort, use Euclidean distance
        assignCluster(cluster, centers, data);

        // iteration
        // (1) re-locate cohort center to average location of users in the cohort
        // (2) re-assign every user to the closest cohort center
        // (3) repeat
        // stop criterion: no change in cluster assignment, or reach max iteration
        int iterMax = 100;
        for (int iter = 0; iter < iterMax; iter++) {
            int[] oldCluster = Arrays.copyOf(cluster, N);
            locateCenters(cluster, centers, data);
            assignCluster(cluster, centers, data);

            // stop
            if (Arrays.equals(oldCluster, cluster)) {
                System.out.println("k-means : clustering converged");
                break;
            }

            if (iter == iterMax) {
                System.out.println("k-means : reach max iteration");
            }
        }

        return cluster;
    }

    private static int randomIntRange(int min, int max) {
        return (int)(Math.random() * (max + 1 - min) + min);
    }

    private static void assignCluster(int[] cluster, final double[][] centers, final double[][] data){
        // N samples, M features
        int N = data.length;
        int M = data[0].length;
        int numCluster = centers.length;

        for (int n = 0; n < N; n++) {
            double minDistance = Double.MAX_VALUE;
            for (int c = 0; c < numCluster; c++) {
                double distance = EuclideanDistance(data[n], centers[c]);
                if (minDistance > distance) {
                    minDistance = distance;
                    cluster[n] = c;
                }
            }
        }
    }

    private static void locateCenters(final int[] cluster, double[][] centers, final double[][] data) {
        // N samples, M features
        int N = data.length;
        int M = data[0].length;
        int numCluster = centers.length;

        // reset centers to 0
        for (int c = 0; c < numCluster; c++) {
            Arrays.fill(centers[c], 0);
        }

        int[] countCluster = new int[numCluster];

        // sum over all samples
        for (int n = 0; n < N; n++) {
            vectorSumInPlace(centers[cluster[n]], data[n]);
            countCluster[cluster[n]]++;
        }

        // divide by number of samples in each cluster
        for (int c = 0; c < numCluster; c++) {
            vectorDivideInPlace(centers[c], countCluster[c]);
        }
    }

    private static double EuclideanDistance(double[] a, double[] b){
        if (a.length != b.length) System.out.println("Error : Euclidean distance, the two vectors must have same dimension!");
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += Math.pow(a[i] - b[i], 2);
        }
        return Math.sqrt(sum);
    }

    private static void vectorSumInPlace(double[] result, double[] add) {
        for (int i = 0; i < result.length; i++) {
            result[i] += add[i];
        }
    }

    private static void vectorDivideInPlace(double[] result, double factor) {
        for (int i = 0; i < result.length; i++) {
            result[i] /= factor;
        }
    }

    public static void test() {
        double[] a = new double[]{3.2, 4.0, -2};
        double[] b = new double[]{2, -5, 1.5};

        vectorSumInPlace(a,b);
        printVector(a);

        vectorDivideInPlace(b,2);
        printVector(b);

        System.out.println(EuclideanDistance(a,b));

        for (int i = 0; i < 1000; i++) {
            int x = randomIntRange(10,20);
            System.out.println(x);
            if (x < 10 || x > 100) {
                System.out.println("error");
                break;
            }
        }

        double[][] data = {{-0.1,0},{0.2,0.3},{-0.1,0.5},{2.1,3.5},{2,3},{2.5,4},};
        run(data, 2);
    }

    private static void printVector(double[] x) {
        System.out.print(x.length + " : ");
        for (int i = 0; i < x.length; i++) {
            System.out.print(x[i] + " ");
        }
        System.out.println();
    }
}
