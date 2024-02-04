import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class ConcurrentMergeSort {

    private static void merge(List<Integer> currList, List<Integer> leftSubList, List<Integer> rightSubList) {

        int[] tmp = new int[currList.size()];

        int leftIdx = 0;
        int rightIdx = 0;
        for (int i = 0; i < currList.size(); i++) {
            Integer leftValue = leftIdx < leftSubList.size() ? leftSubList.get(leftIdx) : null;
            Integer rightValue = rightIdx < rightSubList.size() ? rightSubList.get(rightIdx) : null;

            // To understand this, remember that condition evaluation is from left to right.
            if (leftValue == null || rightValue != null && rightValue < leftValue) {
                // If leftValue is null then rightValue cannot be null.
                tmp[i] = rightValue;
                rightIdx++;
            } else {
                tmp[i] = leftValue;
                leftIdx++;
            }
        }

        for (int i = 0; i < currList.size(); i++) {
            currList.set(i, tmp[i]);
        }
    }

    private static class SplitAndMerge extends RecursiveAction {

        private final List<Integer> currList;

        private SplitAndMerge(List<Integer> data) {
            this.currList = data;
        }

        @Override
        protected void compute() {
            // These print's ideally should be under mutex, but it would additionally slow down the algorithm.
            // Naturally for benchmarking these print's should be commented out.
            System.out.printf("%s PRE split data:%s\n", Thread.currentThread().getName(), currList);
            int sublistSize = currList.size();

            if (sublistSize == 1) {
                return;
            }

            // if subListSize is odd left and right work size is equal else right work size is greater by one.
            int middle = sublistSize / 2;

            List<Integer> leftSubList = currList.subList(0, middle);
            var leftWork = new SplitAndMerge(leftSubList);
            leftWork.fork();
            List<Integer> rightSubList = currList.subList(middle, sublistSize);
            var rightWork = new SplitAndMerge(rightSubList);
            rightWork.compute();
            leftWork.join();

            System.out.printf("%s PRE merge left:%s right:%s\n", Thread.currentThread().getName(), leftSubList, rightSubList);
            merge(currList, leftSubList, rightSubList);
            System.out.printf("%s POST merge data:%s\n", Thread.currentThread().getName(), currList);
        }
    }

    public static void main(String[] args) {

        List<Integer> data = Arrays.asList(47, 45, 42, 40, 38, 10, 8, 6, 4, 3, 1, 0, 0, 28, 26, 24, 20, 100, 120, 80);

        ForkJoinPool pool = new ForkJoinPool();

        try {
            var work = new SplitAndMerge(data);
            pool.invoke(work);
        } finally {
            pool.shutdown();
        }
    }
}
