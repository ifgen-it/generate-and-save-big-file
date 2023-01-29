package com.luxoft;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

/**
 * You need to write to a file the sequence of 100,000,000 bytes generated by java Random class with seed = 0.
 * In the fastest way possible.
 */
public class Worker {
    private static int N_THREADS = Runtime.getRuntime().availableProcessors()/2;
    private static ExecutorService EXECUTOR = Executors.newFixedThreadPool(N_THREADS);
    private static String DEFAULT_CATALOG = "results";

    private Random random;
    private int fileSize;
    private String catalog;
    public Worker(long seed, int fileSize, String catalog) {
        random = new Random(seed);
        if (catalog == null || "".equals(catalog.trim()))
            catalog = DEFAULT_CATALOG;
        File dir = new File(catalog);
        dir.mkdir();
        this.catalog = catalog + "/";
        this.fileSize = fileSize;
    }

    // Generating
    public byte[] generateBytesLib() { // fastest generate
        long tStart = System.currentTimeMillis();
        byte[] bytes = new byte[fileSize];
        random.nextBytes(bytes);
        long tEnd = System.currentTimeMillis();
        logTime("generateBytesLib", tStart, tEnd);
        return bytes;
    }
    public byte[] generateBytes() {
        long tStart = System.currentTimeMillis();
        byte[] bytes = new byte[fileSize];
        for (int i = 0; i < fileSize; i++) {
            bytes[i] = generateByte();
        }
        long tEnd = System.currentTimeMillis();
        logTime("generateBytes", tStart, tEnd);
        return bytes;
    }
    public byte[] generateBytesParallel(Random random) {
        long tStart = System.currentTimeMillis();
        byte[] bytes = new byte[fileSize];
        List<BatchIndex> batches = getBatches();
        List<CompletableFuture<Void>> futures = batches.stream()
                .map(batch -> CompletableFuture.runAsync(() -> {
                    //System.out.println("started thread = " + Thread.currentThread().getName());
                    for (int i = batch.getStart(); i < batch.getEnd(); i++) {
                        bytes[i] = generateByte();
                    }
                }, EXECUTOR)).toList();
        futures.forEach(CompletableFuture::join);
        long tEnd = System.currentTimeMillis();
        logTime("generateBytesParallel", tStart, tEnd);
        EXECUTOR.shutdown();
        return bytes;
    }
    public byte[] generateBytesParallel2(Random random) {
        long tStart = System.currentTimeMillis();
        byte[] bytes = new byte[fileSize];
        List<BatchIndex> batches = getBatches();
        List<CompletableFuture<byte[]>> futures = batches.stream()
                .map(batch -> CompletableFuture.supplyAsync(() -> {
                    //System.out.println("started thread = " + Thread.currentThread().getName());
                    int size = batch.getEnd() - batch.getStart();
                    byte[] batchResult = new byte[size];
                    random.nextBytes(batchResult);
                    return batchResult;
                }, EXECUTOR)).toList();
        futures.forEach(CompletableFuture::join);
        int counter = 0;
        for (CompletableFuture<byte[]> future : futures) {
            try {
                byte[] byteResult = future.get();
                for (byte b : byteResult) {
                    bytes[counter++] = b;
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        long tEnd = System.currentTimeMillis();
        logTime("generateBytesParallel", tStart, tEnd);
        EXECUTOR.shutdown();
        return bytes;
    }
    public byte[] generateBytesParallel3(Random random) {
        long tStart = System.currentTimeMillis();
        byte[] bytes = new byte[fileSize];
        List<BatchIndex> batches = getBatches();
        List<Future<byte[]>> futures = new ArrayList<>();
        for (BatchIndex batch : batches) {
            Future<byte[]> future = EXECUTOR.submit(() -> {
                //System.out.println("started thread = " + Thread.currentThread().getName());
                int size = batch.getEnd() - batch.getStart();
                byte[] batchResult = new byte[size];
                random.nextBytes(batchResult);
                return batchResult;
            });
            futures.add(future);
        }
        int counter = 0;
        for (Future<byte[]> future : futures) {
            try {
                byte[] byteResult = future.get();
                for (byte b : byteResult) {
                    bytes[counter++] = b;
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        long tEnd = System.currentTimeMillis();
        logTime("generateBytesParallel", tStart, tEnd);
        EXECUTOR.shutdown();
        return bytes;
    }
    public Byte[] generateBytesStreamParallel() {
        long tStart = System.currentTimeMillis();
        Byte[] bytes = IntStream.range(0, fileSize).boxed()
                .map(number -> generateByte())
                .toArray(Byte[]::new);
        long tEnd = System.currentTimeMillis();
        logTime("generateBytesStreamParallel", tStart, tEnd);
        return bytes;
    }

    // Writing
    public String writeRandomAccessFileParallel(byte[] bytes) {
        long tStart = System.currentTimeMillis();
        String opName = "writeRandomAccessFileParallel";
        String fileName = getFileName(opName);

        List<BatchIndex> batches = getBatches();
        List<CompletableFuture<Void>> futures = batches.stream()
                .map(batch -> CompletableFuture.runAsync(() -> {
                    try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
                        try {
                            int start = batch.getStart();
                            int length = batch.getEnd() - batch.getStart();
                            raf.seek(start);
                            raf.write(bytes, start, length);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } catch (IOException ex) {
                        System.out.println(ex.getMessage());
                    }
                }, EXECUTOR))
                .toList();
        futures.forEach(CompletableFuture::join);
        EXECUTOR.shutdown();
        long tEnd = System.currentTimeMillis();
        logTime(opName, tStart, tEnd);
        return fileName;
    }
    public String writeFileOutputStream(byte[] bytes) {
        long tStart = System.currentTimeMillis();
        String opName = "writeFileOutputStream";
        String fileName = getFileName(opName);
        try (FileOutputStream fos = new FileOutputStream(fileName)) {
            fos.write(bytes);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
        long tEnd = System.currentTimeMillis();
        logTime(opName, tStart, tEnd);
        return fileName;
    }
    public String writeRandomAccessFile(byte[] bytes) {
        long tStart = System.currentTimeMillis();
        String opName = "writeRandomAccessFile";
        String fileName = getFileName(opName);
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
            raf.write(bytes);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
        long tEnd = System.currentTimeMillis();
        logTime(opName, tStart, tEnd);
        return fileName;
    }

    public String writeMappedByteBuffer(byte[] bytes) {
        long tStart = System.currentTimeMillis();
        String opName = "writeMappedByteBuffer";
        String fileName = getFileName(opName);
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
            FileChannel channel = raf.getChannel();
            MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            mbb.put(bytes);

        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
        long tEnd = System.currentTimeMillis();
        logTime(opName, tStart, tEnd);
        return fileName;
    }

    public String writeMappedByteBufferParallel(byte[] bytes) {
        long tStart = System.currentTimeMillis();
        String opName = "writeMappedByteBufferParallel";
        String fileName = getFileName(opName);

        List<BatchIndex> batches = getBatches();
        List<CompletableFuture<Void>> futures = batches.stream()
                .map(batch -> CompletableFuture.runAsync(() -> {
                    try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
                        try {
                            FileChannel channel = raf.getChannel();
                            MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
                            int start = batch.getStart();
                            int length = batch.getEnd() - batch.getStart();

                            mbb.put(start, bytes, start, length);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } catch (IOException ex) {
                        System.out.println(ex.getMessage());
                    }
                }, EXECUTOR))
                .toList();
        futures.forEach(CompletableFuture::join);
        EXECUTOR.shutdown();
        long tEnd = System.currentTimeMillis();
        logTime(opName, tStart, tEnd);
        return fileName;
    }

    public String generateAndWriteMappedByteBuffer() { // not-fast
        long tStart = System.currentTimeMillis();
        String opName = "generateAndWriteMappedByteBuffer";
        String fileName = getFileName(opName);
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
            FileChannel channel = raf.getChannel();
            MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            byte[] bytes = generateBytesLib();
            mbb.put(bytes);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
        long tEnd = System.currentTimeMillis();
        logTime(opName, tStart, tEnd);
        return fileName;
    }
    public String generateAndWriteRandomAccessFileParallel() {
        long tStart = System.currentTimeMillis();
        String opName = "generateAndWriteRandomAccessFileParallel";
        String fileName = getFileName(opName);

        List<BatchIndex> batches = getBatches();
        List<CompletableFuture<Void>> futures = batches.stream()
                .map(batch -> CompletableFuture.runAsync(() -> {
                    try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
                        try {
                            int start = batch.getStart();
                            int length = batch.getEnd() - batch.getStart();
                            byte[] bytes = new byte[length];
                            random.nextBytes(bytes);
                            raf.seek(start);
                            raf.write(bytes, 0, length);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } catch (IOException ex) {
                        System.out.println(ex.getMessage());
                    }
                }, EXECUTOR))
                .toList();
        futures.forEach(CompletableFuture::join);
        EXECUTOR.shutdown();
        long tEnd = System.currentTimeMillis();
        logTime(opName, tStart, tEnd);
        return fileName;
    }

    // Reading
    public byte[] readBytesFromFile(String fileName) {
        byte[] bytes = new byte[fileSize];
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {
            try {
                raf.read(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
        return bytes;
    }

    // Utils
    public boolean checkRandomization(byte[] bytes) {
        int[] bytesCount = new int[256];
        for (byte aByte : bytes) {
            int index = (int)aByte + 128;
            bytesCount[index]++;
        }

        List<Integer> notGeneratedNumbers = new ArrayList<>();
        for (int i = 0; i < bytesCount.length; i++) {
            if (bytesCount[i] == 0)
                notGeneratedNumbers.add(i);
        }
        System.out.print("Check randomization: ");
        if (notGeneratedNumbers.isEmpty()) {
            System.out.println("All numbers were generated");
            return true;
        }
        else {
            System.out.println("Numbers, which were not generated:");
            notGeneratedNumbers.forEach(number -> System.out.print(number + " "));
            System.out.println();
            return false;
        }
    }
    private List<BatchIndex> getBatches() {
        List<BatchIndex> result = new ArrayList<>();
        int start = 0;
        int batchSize = fileSize / N_THREADS;
        while (true) {
            int end = Math.min(start + batchSize, fileSize);
            result.add(new BatchIndex(start, end));
            if (end == fileSize)
                break;
            start = end;
        }
        return result;
    }
    private byte generateByte() {
        int number = random.nextInt(256) - 128;
        return (byte)number;
    }
    public void logTime(String operationName, long tStart, long tEnd) {
        System.out.println(String.format("Operation: %s - execution time = %d ms", operationName, tEnd - tStart));
    }

    private String getFileName(String operationName) {
        return catalog + operationName + "-" + getTimeSuffix() + ".txt";
    }
    private String getTimeSuffix() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH-mm-ss"));
    }
    private static class BatchIndex {
        private int start;
        private int end;

        public BatchIndex(int start, int end) {
            this.start = start;
            this.end = end;
        }

        public int getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }
    }

}
