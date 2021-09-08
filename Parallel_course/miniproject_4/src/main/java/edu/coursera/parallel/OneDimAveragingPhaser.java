package edu.coursera.parallel;

import java.util.concurrent.Phaser;

/**
 * Wrapper class for implementing one-dimensional iterative averaging using
 * phasers.
 */
public final class OneDimAveragingPhaser {
    /**
     * Default constructor.
     */
    private OneDimAveragingPhaser() {
    }

    /**
     * Sequential implementation of one-dimensional iterative averaging.
     *
     * @param iterations The number of iterations to run
     * @param myNew A double array that starts as the output array
     * @param myVal A double array that contains the initial input to the
     *        iterative averaging problem
     * @param n The size of this problem
     */
    public static void runSequential(final int iterations, final double[] myNew,
            final double[] myVal, final int n) {
        double[] next = myNew;
        double[] curr = myVal;

        for (int iter = 0; iter < iterations; iter++) {
            for (int j = 1; j <= n; j++) {
                next[j] = (curr[j - 1] + curr[j + 1]) / 2.0;
            }
            double[] tmp = curr;
            curr = next;
            next = tmp;
        }
    }

    /**
     * An example parallel implementation of one-dimensional iterative averaging
     * that uses phasers as a simple barrier (arriveAndAwaitAdvance).
     *
     * @param iterations The number of iterations to run
     * @param myNew A double array that starts as the output array
     * @param myVal A double array that contains the initial input to the
     *        iterative averaging problem
     * @param n The size of this problem
     * @param tasks The number of threads/tasks to use to compute the solution
     */
    public static void runParallelBarrier(final int iterations,
            final double[] myNew, final double[] myVal, final int n,
            final int tasks) {
        Phaser ph = new Phaser(0);
        ph.bulkRegister(tasks);

        Thread[] threads = new Thread[tasks];

        for (int ii = 0; ii < tasks; ii++) {
            final int i = ii;

            threads[ii] = new Thread(() -> {
                double[] threadPrivateMyVal = myVal;
                double[] threadPrivateMyNew = myNew;

                final int chunkSize = (n + tasks - 1) / tasks;
                final int left = (i * chunkSize) + 1;
                int right = (left + chunkSize) - 1;
                if (right > n) right = n;

                for (int iter = 0; iter < iterations; iter++) {
                    for (int j = left; j <= right; j++) {
                        threadPrivateMyNew[j] = (threadPrivateMyVal[j - 1]
                            + threadPrivateMyVal[j + 1]) / 2.0;
                    }
                    ph.arriveAndAwaitAdvance();

                    double[] temp = threadPrivateMyNew;
                    threadPrivateMyNew = threadPrivateMyVal;
                    threadPrivateMyVal = temp;
                }
            });
            threads[ii].start();
        }

        for (int ii = 0; ii < tasks; ii++) {
            try {
                threads[ii].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * A parallel implementation of one-dimensional iterative averaging that
     * uses the Phaser.arrive and Phaser.awaitAdvance APIs to overlap
     * computation with barrier completion.
     *
     * TODO Complete this method based on the provided runSequential and
     * runParallelBarrier methods.
     *
     * @param iterations The number of iterations to run
     * @param myNew A double array that starts as the output array
     * @param myVal A double array that contains the initial input to the
     *              iterative averaging problem
     * @param n The size of this problem
     * @param tasks The number of threads/tasks to use to compute the solution
     */
    public static void runParallelFuzzyBarrier(final int iterations,
            final double[] myNew, final double[] myVal, final int n,
            final int tasks) {
//        Phaser ph = new Phaser(0);
//        ph.bulkRegister(tasks);

        // create an array of phasers equal to the number of tasks
        Phaser[] ph_array = new Phaser[tasks];
        // register the phasers
        for(int i=0; i<tasks; i++){
            // since we are instantiating this in the main thread we need
            // to initialise this with 1, see https://www.baeldung.com/java-phaser
            ph_array[i] = new Phaser(1);
            // in this case we can advance if 2 phasers have arrived
            // no need to wait for all the phasers
//            if (i == 0) {
//                ph_array[i].bulkRegister(0);
//            } else if (i == tasks-1) {
//                ph_array[i].bulkRegister(0);
//            } else {
//                ph_array[i].bulkRegister(0);
//            }
        }

//        System.out.println("here");

        Thread[] threads = new Thread[tasks];

        for (int ii = 0; ii < tasks; ii++) {
            final int i = ii;
            // instantiate a new thread to do work in this chunk
            threads[ii] = new Thread(() -> {
                // each thread receives a copy of the reference to the arrays
                double[] threadPrivateMyVal = myVal;
                double[] threadPrivateMyNew = myNew;
                // get the starting point and the ending point of the current chunk
                final int chunkSize = (n + tasks - 1) / tasks;
                final int left = (i * chunkSize) + 1;
                int right = (left + chunkSize) - 1;
                if (right > n) right = n;
                // this is the time step operation in jacobi iteration
                for (int iter = 0; iter < iterations; iter++) {
                    // here we iterate over each element of the array
                    for (int j = left; j <= right; j++) {
                        threadPrivateMyNew[j] = (threadPrivateMyVal[j - 1]
                                + threadPrivateMyVal[j + 1]) / 2.0;
                    }
                    // we have completed one timestep in this thread so we
                    // notify that we arrive
                    ph_array[i].arrive();
                    // if the previous and the next chunks have "arrived" the continue
                    // the loop
                    if(i > 0) {
//                        System.out.println("i = " + (i-1) + " " + ph_array[i-1].getArrivedParties());
                        ph_array[i-1].awaitAdvance(iter);

                    }
                    if(i < tasks-1) {
//                        System.out.println("i = " + (i+1) + " " + ph_array[i+1].getArrivedParties());
                        ph_array[i+1].awaitAdvance(iter);

                    }
                    double[] temp = threadPrivateMyNew;
                    threadPrivateMyNew = threadPrivateMyVal;
                    threadPrivateMyVal = temp;
                }
            });
            threads[ii].start();
        }

        for (int ii = 0; ii < tasks; ii++) {
            try {
                threads[ii].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

//    public static void main(String[] args) {
//        System.out.println("hello world!");
//        double[] myVal = new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
//        double[] myNew = new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
//        System.out.println("aaa");
//        OneDimAveragingPhaser.runParallelFuzzyBarrier(3, myNew, myVal, 9, 3);
//        System.out.println("bbb");
//    }
}
