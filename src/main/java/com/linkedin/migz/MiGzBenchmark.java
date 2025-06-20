package com.linkedin.migz;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class MiGzBenchmark {
    private static final String RESOURCES_DIR = "src/main/resources/";
    private static final Pattern TEST_FILE_PATTERN = Pattern.compile("test_\\d+\\.gz");
    private static final int ITERATIONS = 10;
    private static final int BUFFER_SIZE = 512 * 1024;

    public static void main(String[] args) throws IOException {
        List<Path> testFiles = new ArrayList<>();
        Files.list(Paths.get(RESOURCES_DIR))
                .filter(p -> TEST_FILE_PATTERN.matcher(p.getFileName().toString()).matches())
                .sorted(Comparator.comparing(Path::toString))
                .forEach(testFiles::add);

        if (testFiles.isEmpty()) {
            System.err.println("No test_*.gz files found in " + RESOURCES_DIR);
            return;
        }

        System.out.println("Benchmarking decompression on files: ");
        for (Path p : testFiles) {
            System.out.println("  - " + p.getFileName() + " (" + (Files.size(p) / (1024 * 1024)) + " MB)");
        }
        System.out.println();

        for (Path file : testFiles) {
            System.out.println("==== File: " + file.getFileName() + " ====");
            benchmarkDecompression(file, ITERATIONS);
            System.out.println();
        }
    }

    private static void benchmarkDecompression(Path file, int iterations) throws IOException {
        byte[] fileBytes = Files.readAllBytes(file);
        System.out.println("  [MiGzInputStream]  Iterations: " + iterations);
        runBenchmark(fileBytes, iterations, true);
        System.out.println("  [GZIPInputStream]  Iterations: " + iterations);
        runBenchmark(fileBytes, iterations, false);
    }

    private static void runBenchmark(byte[] fileBytes, int iterations, boolean useMiGz) {
        long totalTime = 0;
        long peakMemory = 0;
        int totalBytes = 0;
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

        for (int i = 0; i < iterations; i++) {
            System.gc();
            try { Thread.sleep(100); } catch (InterruptedException ignored) {}
            long beforeUsedMem = getUsedMemory(memoryBean);
            long start = System.nanoTime();
            int bytes = 0;
            try (InputStream bais = new ByteArrayInputStream(fileBytes)) {
                InputStream in;
                if (useMiGz) {
                    in = new MiGzInputStream(bais, ForkJoinPool.commonPool());
                } else {
                    in = new GZIPInputStream(bais, BUFFER_SIZE);
                }
                byte[] buffer = new byte[BUFFER_SIZE];
                int read;
                while ((read = in.read(buffer)) > 0) {
                    bytes += read;
                }
                in.close();
            } catch (Exception e) {
                System.err.println("    Error: " + e.getMessage());
                e.printStackTrace();
                return;
            }
            long elapsed = System.nanoTime() - start;
            long afterUsedMem = getUsedMemory(memoryBean);
            long memUsed = afterUsedMem - beforeUsedMem;
            if (memUsed > peakMemory) peakMemory = memUsed;
            totalTime += elapsed;
            totalBytes = bytes; // should be the same every time
        }
        System.out.printf("    Avg Time: %.2f ms\n", totalTime / 1e6 / iterations);
        System.out.printf("    Peak Memory: %.2f MB\n", peakMemory / (1024.0 * 1024.0));
        System.out.printf("    Output Size: %d bytes\n", totalBytes);
    }

    private static long getUsedMemory(MemoryMXBean memoryBean) {
        MemoryUsage heap = memoryBean.getHeapMemoryUsage();
        MemoryUsage nonHeap = memoryBean.getNonHeapMemoryUsage();
        return heap.getUsed() + nonHeap.getUsed();
    }
} 