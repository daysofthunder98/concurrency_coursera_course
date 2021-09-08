package edu.coursera.parallel;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.ArrayList;
import java.util.concurrent.ForkJoinTask;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // long startTime = System.nanoTime();

        // Compute sum of reciprocals of array elements
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        // long endTime = System.nanoTime();
        // System.out.println("This took " + (endTime-startTime) / 1000000 + " milliseconds");

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the start of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     *         nElements
     */
    private static int getChunkStartInclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the end of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value;

        /**
         * Constructor.
         * @param setStartIndexInclusive Set the starting index to begin
         *        parallel traversal at.
         * @param setEndIndexExclusive Set ending index for parallel traversal.
         * @param setInput Input values
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                final int setEndIndexExclusive, final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
        }

        /**
         * Getter for the value produced by this task.
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {
            value = 0;
            for(int i = startIndexInclusive; i <= endIndexExclusive; i++) {
                value += 1 / input[i];
            }
        }
    }

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(final double[] input) {
        // long startTime = System.nanoTime();
        assert input.length % 2 == 0;

        double sum = 0;

        // ForkJoinPool pool = new ForkJoinPool(2);

        // Compute sum of reciprocals of array elements
        int mid = input.length / 2;
        // split the array into two halves
        ReciprocalArraySumTask left_arr_sum = new ReciprocalArraySumTask(0, mid, input);
        ReciprocalArraySumTask right_arr_sum = new ReciprocalArraySumTask(mid+1, input.length-1, input);
        // invoke the left sum asynchronously
        left_arr_sum.fork();
        // compute the right sum
        right_arr_sum.compute();
        left_arr_sum.join();
        sum = left_arr_sum.getValue() + right_arr_sum.getValue();

        // long endTime = System.nanoTime();
        // System.out.println("This took " + (endTime-startTime) / 1000000 + " milliseconds");
        return sum;
    }

    /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,
            final int numTasks) {
        // long startTime = System.nanoTime();
        double sum = 0;
        ArrayList<ReciprocalArraySumTask> lst = new ArrayList<>();
//        ForkJoinPool pool = new ForkJoinPool(numTasks);
        // split the array into number of chunks = numTasks
        for(int chunk = 0; chunk < numTasks; chunk++) {
            // get the begin and the end inclusive indices
            int start_idx = ReciprocalArraySum.getChunkStartInclusive(chunk, numTasks, input.length);
            int end_idx = ReciprocalArraySum.getChunkEndExclusive(chunk, numTasks, input.length)-1;
            // create an object and add it to the list to invoke later
            ReciprocalArraySumTask current_task = new ReciprocalArraySumTask(start_idx, end_idx, input);
            lst.add(current_task);
        }
        // do an invokeall on the divided chunks of the array
//        ForkJoinPool pool = new ForkJoinPool(numTasks);
//        ForkJoinTask.invokeAll(lst);
        ForkJoinTask.invokeAll(lst);
        // now sum all the returns
        for (ReciprocalArraySumTask some: lst) {
            sum += some.getValue();
        }
        // long endTime = System.nanoTime();
        // System.out.println("This took " + (endTime-startTime) / 1000000 + " milliseconds");
        return sum;
    }

//    public static void main(String[] args) {
//        // ReciprocalArraySum some = new ReciprocalArraySum();
//        int size = 200000000;
//        double input[] = new double[size];
//        // fill the array
//        for (int i = 0; i < input.length; i++) {
//            input[i] = i+1;
//        }
//        System.out.println(ReciprocalArraySum.seqArraySum(input));
//
//        System.out.println(ReciprocalArraySum.parManyTaskArraySum(input, 2));
//    }
}
