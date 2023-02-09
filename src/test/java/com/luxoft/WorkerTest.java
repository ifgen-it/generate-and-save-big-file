package com.luxoft;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class WorkerTest {

    @Test
    public void test_generate_and_write_mapped_byte_buffer_batches() {
        final int len = 100_000_000;
        Random rand0 = new Random(0);
        byte[] bytesRand0 = new byte[len];
        rand0.nextBytes(bytesRand0);

        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, 100_000_000, "results");
        String fileName = worker.generateAndWriteMappedByteBufferBatches();
        long tEnd = System.currentTimeMillis();
        worker.logTime("wholeProgram", tStart, tEnd);

        byte[] readBytes = worker.readBytesFromFile(fileName);
        Assertions.assertArrayEquals(bytesRand0, readBytes);
    }
    @Test
    public void test_generate_and_write_mapped_byte_buffer_queue() {
        final int len = 100_000_000;
        Random rand0 = new Random(0);
        byte[] bytesRand0 = new byte[len];
        rand0.nextBytes(bytesRand0);

        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, 100_000_000, "results");
        String fileName = worker.generateAndWriteMappedByteBufferBatchesQueue();
        long tEnd = System.currentTimeMillis();
        worker.logTime("wholeProgram", tStart, tEnd);

        byte[] readBytes = worker.readBytesFromFile(fileName);
        Assertions.assertArrayEquals(bytesRand0, readBytes);
    }

    @Test
    public void test_serial_generating_parallel_writing_mapped_byte_buffer() {
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, 100_000_000, "results");
        byte[] bytes = worker.generateBytesLib();
        String fileName = worker.writeMappedByteBufferParallel(bytes);
        long tEnd = System.currentTimeMillis();
        worker.logTime("wholeProgram", tStart, tEnd);

        byte[] readBytes = worker.readBytesFromFile(fileName);
        Assertions.assertArrayEquals(bytes, readBytes);
        Assertions.assertTrue(worker.checkRandomization(bytes));
    }

    @Test
    public void test_serial_generating_serial_writing_mapped_byte_buffer() {
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, 100_000_000, "results");
        byte[] bytes = worker.generateBytesLib();
        String fileName = worker.writeMappedByteBuffer(bytes);
        long tEnd = System.currentTimeMillis();
        worker.logTime("wholeProgram", tStart, tEnd);

        byte[] readBytes = worker.readBytesFromFile(fileName);
        Assertions.assertArrayEquals(bytes, readBytes);
        Assertions.assertTrue(worker.checkRandomization(bytes));
    }

    @Test
    public void test_serial_generating_parallel_writing_random_access_file() {
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, 100_000_000, "results");
        byte[] bytes = worker.generateBytesLib();
        String fileName = worker.writeRandomAccessFileParallel(bytes);
        long tEnd = System.currentTimeMillis();
        worker.logTime("wholeProgram", tStart, tEnd);

        byte[] readBytes = worker.readBytesFromFile(fileName);
        Assertions.assertArrayEquals(bytes, readBytes);
        Assertions.assertTrue(worker.checkRandomization(bytes));
    }

    @Test
    public void test_serial_generating_serial_writing_random_access_file() {
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, 100_000_000, "results");
        byte[] bytes = worker.generateBytesLib();
        String fileName = worker.writeRandomAccessFile(bytes);
        long tEnd = System.currentTimeMillis();
        worker.logTime("wholeProgram", tStart, tEnd);

        byte[] readBytes = worker.readBytesFromFile(fileName);
        Assertions.assertArrayEquals(bytes, readBytes);
        Assertions.assertTrue(worker.checkRandomization(bytes));
    }
    @Test
    public void test_serial_generating_serial_writing_file_output_stream() {
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, 100_000_000, "results");
        byte[] bytes = worker.generateBytesLib();
        String fileName = worker.writeFileOutputStream(bytes);
        long tEnd = System.currentTimeMillis();
        worker.logTime("wholeProgram", tStart, tEnd);

        byte[] readBytes = worker.readBytesFromFile(fileName);
        Assertions.assertArrayEquals(bytes, readBytes);
        Assertions.assertTrue(worker.checkRandomization(bytes));
    }

    /**
     * Parallel generating generates bytes incorrectly
     */
    @Test
    public void test_parallel_generating_is_incorrect() {
        final int len = 100_000_000;

        Random rand0 = new Random(0);
        byte[] bytesRand0 = new byte[len];
        rand0.nextBytes(bytesRand0);
        Random rand1 = new Random(0);
        byte[] bytesRand1 = new byte[len];
        rand1.nextBytes(bytesRand1);
        Assertions.assertArrayEquals(bytesRand0, bytesRand1);

        Worker worker1 = new Worker(0, len, "results");
        Worker worker2 = new Worker(0, len, "results");
        byte[] bytesLibParallel1 = worker1.generateBytesLibParallel();
        byte[] bytesLibParallel2 = worker2.generateBytesLibParallel();
        Assertions.assertNotEquals(bytesLibParallel1, bytesLibParallel2);
        Assertions.assertNotEquals(bytesRand0, bytesLibParallel2);
    }

}
