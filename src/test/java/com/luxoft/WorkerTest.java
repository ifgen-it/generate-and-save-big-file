package com.luxoft;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
public class WorkerTest {

    @Test
    public void test_serial_generating_parallel_writing_mapped_byte_buffer() { // 1st place
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, 100_000_000, "results");
        byte[] bytes = worker.generateBytesLib();                       // 238 ms
        String fileName = worker.writeMappedByteBufferParallel(bytes);  // 32 ms
        long tEnd = System.currentTimeMillis();
        worker.logTime("wholeProgram", tStart, tEnd);

        byte[] readBytes = worker.readBytesFromFile(fileName);
        Assertions.assertArrayEquals(bytes, readBytes);
        Assertions.assertTrue(worker.checkRandomization(bytes));
    }

    @Test
    public void test_serial_generating_serial_writing_mapped_byte_buffer() { // 2nd place
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, 100_000_000, "results");
        byte[] bytes = worker.generateBytesLib();
        String fileName = worker.writeMappedByteBuffer(bytes); // 53 ms
        long tEnd = System.currentTimeMillis();
        worker.logTime("wholeProgram", tStart, tEnd);

        byte[] readBytes = worker.readBytesFromFile(fileName);
        Assertions.assertArrayEquals(bytes, readBytes);
        Assertions.assertTrue(worker.checkRandomization(bytes));
    }

    @Test
    public void test_serial_generating_parallel_writing_random_access_file() { // 3rd place
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, 100_000_000, "results");
        byte[] bytes = worker.generateBytesLib();
        String fileName = worker.writeRandomAccessFileParallel(bytes); // 67 ms
        long tEnd = System.currentTimeMillis();
        worker.logTime("wholeProgram", tStart, tEnd);

        byte[] readBytes = worker.readBytesFromFile(fileName);
        Assertions.assertArrayEquals(bytes, readBytes);
        Assertions.assertTrue(worker.checkRandomization(bytes));
    }

    @Test
    public void test_serial_generating_serial_writing_random_access_file() { // 4rd place
        long tStart = System.currentTimeMillis();
        Worker worker = new Worker(0, 100_000_000, "results");
        byte[] bytes = worker.generateBytesLib();
        String fileName = worker.writeRandomAccessFile(bytes); // 121 ms
        long tEnd = System.currentTimeMillis();
        worker.logTime("wholeProgram", tStart, tEnd);

        byte[] readBytes = worker.readBytesFromFile(fileName);
        Assertions.assertArrayEquals(bytes, readBytes);
        Assertions.assertTrue(worker.checkRandomization(bytes));
    }

}
