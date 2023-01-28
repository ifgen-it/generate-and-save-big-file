package com.luxoft;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
public class WorkerTest {

    @Test
    public void test_fastest_serial_generating_parallel_writing() {
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

}
