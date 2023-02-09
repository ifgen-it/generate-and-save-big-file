package com.luxoft;

import org.junit.jupiter.api.Test;

import java.util.*;

public class WorkerCompareMethodsTest {

    /**
     * Pay attention at variable numberOfRepetitions!
     * For each repetition will be saved 6 files, summary size ~ 6*100 Mb,
     * so if numberOfRepetitions = 20, summary saved files size will be ~ 12 Gb
     */
    @Test
    public void test_compare_average_times() {
        Map<String, List<Long>> times = new HashMap<>();
        int fileSize = 100_000_000;
        String method;
        long time;
        int repeatToStartSaveResults = 4; // use value > 0 to save results on hot processor
        int numberOfRepetitions = 20;
        for (int repeat = 0; repeat < numberOfRepetitions; repeat++) {
            method = "1.WriteMappedByteBufferParallel";
            time = getTimeWriteMappedByteBufferParallel(fileSize);
            saveTime(repeatToStartSaveResults, repeat, method, time, times);

            method = "2.GenerateAndWriteMappedByteBufferBatches";
            time = getTimeGenerateAndWriteMappedByteBufferBatches(fileSize);
            saveTime(repeatToStartSaveResults, repeat, method, time, times);

            method = "3.WriteMappedByteBuffer";
            time = getTimeWriteMappedByteBuffer(fileSize);
            saveTime(repeatToStartSaveResults, repeat, method, time, times);

            method = "4.GenerateAndWriteMappedByteBufferQueue";
            time = getTimeGenerateAndWriteMappedByteBufferQueue(fileSize);
            saveTime(repeatToStartSaveResults, repeat, method, time, times);

            method = "5.WriteRandomAccessFile";
            time = getTimeWriteRandomAccessFile(fileSize);
            saveTime(repeatToStartSaveResults, repeat, method, time, times);

            method = "6.WriteFileOutputStream";
            time = getTimeWriteFileOutputStream(fileSize);
            saveTime(repeatToStartSaveResults, repeat, method, time, times);
        }
        printTimes(times);
    }

    private void printTimes(Map<String, List<Long>> times) {
        times.entrySet().stream()
                .map(entry -> {
                    String method = entry.getKey();
                    List<Long> timeList = entry.getValue();
                    Long averageTime = timeList.stream().reduce(Long::sum).map(sum -> sum / timeList.size())
                            .orElseThrow(() -> new RuntimeException("Error in counting average test-time"));
                    Long minTime = timeList.stream().reduce(Long::min)
                            .orElseThrow(() -> new RuntimeException("Error in counting min test-time"));
                    return new ResultTime(method, averageTime, minTime, timeList);
                })
                .sorted(Comparator.comparing(ResultTime::getAverageTime))
                .forEach(resultTime -> {
                    System.out.println("*****************************************");
                    System.out.println(resultTime.getMethod() + ":");
                    System.out.println("Average time, ms:   " + resultTime.getAverageTime());
                    System.out.println("Minimum time, ms:   " + resultTime.getMinTime());
                    System.out.println("All times list, ms: " + resultTime.getTimeList());
                });
    }

    private void saveTime(int repeatToStart, int repeat, String method, long time, Map<String, List<Long>> times) {
        if (repeat < repeatToStart)
            return;
        times.putIfAbsent(method, new ArrayList<>());
        times.get(method).add(time);
    }

    private long getTimeGenerateAndWriteMappedByteBufferBatches(Integer fileSize) {
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, fileSize, "results");
        String fileName = worker.generateAndWriteMappedByteBufferBatches();
        long tEnd = System.currentTimeMillis();
        return tEnd - tStart;
    }
    private long getTimeGenerateAndWriteMappedByteBufferQueue(Integer fileSize) {
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, fileSize, "results");
        String fileName = worker.generateAndWriteMappedByteBufferBatchesQueue();
        long tEnd = System.currentTimeMillis();
        return tEnd - tStart;
    }
    private long getTimeWriteMappedByteBufferParallel(Integer fileSize) {
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, fileSize, "results");
        byte[] bytes = worker.generateBytesLib();                       // 238 ms
        String fileName = worker.writeMappedByteBufferParallel(bytes);
        long tEnd = System.currentTimeMillis();
        return tEnd - tStart;
    }
    private long getTimeWriteMappedByteBuffer(Integer fileSize) {
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, fileSize, "results");
        byte[] bytes = worker.generateBytesLib();                       // 238 ms
        String fileName = worker.writeMappedByteBuffer(bytes);
        long tEnd = System.currentTimeMillis();
        return tEnd - tStart;
    }
    private long getTimeWriteRandomAccessFile(Integer fileSize) {
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, fileSize, "results");
        byte[] bytes = worker.generateBytesLib();                       // 238 ms
        String fileName = worker.writeRandomAccessFile(bytes);
        long tEnd = System.currentTimeMillis();
        return tEnd - tStart;
    }
    private long getTimeWriteFileOutputStream(Integer fileSize) {
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, fileSize, "results");
        byte[] bytes = worker.generateBytesLib();                       // 238 ms
        String fileName = worker.writeFileOutputStream(bytes);
        long tEnd = System.currentTimeMillis();
        return tEnd - tStart;
    }

    private static class ResultTime {
        private String method;
        private long averageTime;
        private long minTime;
        private List<Long> timeList;

        public ResultTime(String method, long averageTime, long minTime, List<Long> timeList) {
            this.method = method;
            this.averageTime = averageTime;
            this.minTime = minTime;
            this.timeList = timeList;
        }

        public String getMethod() {
            return method;
        }

        public long getAverageTime() {
            return averageTime;
        }

        public long getMinTime() {
            return minTime;
        }

        public List<Long> getTimeList() {
            return timeList;
        }
    }
}
