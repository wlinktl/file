package com.demo.file;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/**
 * Benchmark for GZIP decompression performance.
 * Compares different approaches:
 * 1. Standard single-threaded GZIPInputStream
 * 2. FastGzipDecompressor (optimized buffered I/O)
 * 3. AdvancedGzipDecompressor (memory-mapped + streaming)
 */
public class GzipDecompressionBenchmark {
    
    private static final int BENCHMARK_RUNS = 3;
    
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java GzipDecompressionBenchmark <input.gz> [output_dir]");
            System.out.println("Example: java GzipDecompressionBenchmark large_file.gz ./output");
            return;
        }
        
        String inputFile = args[0];
        String outputDir = args.length > 1 ? args[1] : "./benchmark_output";
        
        try {
            // Create output directory
            Path outputPath = Paths.get(outputDir);
            if (!Files.exists(outputPath)) {
                Files.createDirectories(outputPath);
            }
            
            // Run benchmark
            runBenchmark(inputFile, outputDir);
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Run benchmark comparing all approaches.
     */
    public static void runBenchmark(String inputFile, String outputDir) throws IOException {
        Path inputPath = Paths.get(inputFile);
        
        if (!Files.exists(inputPath)) {
            throw new FileNotFoundException("Input file not found: " + inputFile);
        }
        
        long fileSize = Files.size(inputPath);
        System.out.println("=== GZIP Decompression Performance Benchmark ===");
        System.out.println("Input file: " + inputFile);
        System.out.println("File size: " + formatFileSize(fileSize));
        System.out.println("Output directory: " + outputDir);
        System.out.println("Benchmark runs: " + BENCHMARK_RUNS);
        System.out.println();
        
        // Test 1: Standard single-threaded decompression
        System.out.println("1. Standard Single-threaded Decompression");
        BenchmarkResult standardResult = benchmarkStandardDecompression(inputFile, outputDir + "/standard_output.txt");
        printResult("Standard", standardResult);
        
        // Test 2: FastGzipDecompressor
        System.out.println("\n2. FastGzipDecompressor (Optimized Buffered I/O)");
        BenchmarkResult fastResult = benchmarkFastDecompressor(inputFile, outputDir + "/fast_output.txt");
        printResult("Fast", fastResult);
        
        // Test 3: AdvancedGzipDecompressor
        System.out.println("\n3. AdvancedGzipDecompressor (Memory-mapped + Streaming)");
        BenchmarkResult advancedResult = benchmarkAdvancedDecompressor(inputFile, outputDir + "/advanced_output.txt");
        printResult("Advanced", advancedResult);
        
        // Summary
        printSummary(standardResult, fastResult, advancedResult);
        
        // Verify all outputs are identical
        verifyOutputs(outputDir);
    }
    
    /**
     * Benchmark standard single-threaded decompression.
     */
    private static BenchmarkResult benchmarkStandardDecompression(String inputFile, String outputFile) throws IOException {
        long[] times = new long[BENCHMARK_RUNS];
        long decompressedSize = 0;
        
        for (int i = 0; i < BENCHMARK_RUNS; i++) {
            long startTime = System.nanoTime();
            decompressStandard(inputFile, outputFile + "." + i);
            long endTime = System.nanoTime();
            times[i] = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
            
            if (i == 0) {
                decompressedSize = Files.size(Paths.get(outputFile + "." + i));
            }
        }
        
        return calculateStatistics(times, decompressedSize);
    }
    
    /**
     * Benchmark FastGzipDecompressor.
     */
    private static BenchmarkResult benchmarkFastDecompressor(String inputFile, String outputFile) throws IOException {
        long[] times = new long[BENCHMARK_RUNS];
        long decompressedSize = 0;
        
        for (int i = 0; i < BENCHMARK_RUNS; i++) {
            long startTime = System.nanoTime();
            try (FastGzipDecompressor decompressor = new FastGzipDecompressor()) {
                decompressor.decompressFile(inputFile, outputFile + "." + i);
            }
            long endTime = System.nanoTime();
            times[i] = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
            
            if (i == 0) {
                decompressedSize = Files.size(Paths.get(outputFile + "." + i));
            }
        }
        
        return calculateStatistics(times, decompressedSize);
    }
    
    /**
     * Benchmark AdvancedGzipDecompressor.
     */
    private static BenchmarkResult benchmarkAdvancedDecompressor(String inputFile, String outputFile) throws IOException {
        long[] times = new long[BENCHMARK_RUNS];
        long decompressedSize = 0;
        
        for (int i = 0; i < BENCHMARK_RUNS; i++) {
            long startTime = System.nanoTime();
            try (AdvancedGzipDecompressor decompressor = new AdvancedGzipDecompressor()) {
                decompressor.decompressFile(inputFile, outputFile + "." + i);
            }
            long endTime = System.nanoTime();
            times[i] = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
            
            if (i == 0) {
                decompressedSize = Files.size(Paths.get(outputFile + "." + i));
            }
        }
        
        return calculateStatistics(times, decompressedSize);
    }
    
    /**
     * Standard single-threaded decompression.
     */
    private static void decompressStandard(String inputFile, String outputFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputFile);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             FileOutputStream fos = new FileOutputStream(outputFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            
            while ((bytesRead = gzis.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }
        }
    }
    
    /**
     * Calculate statistics from benchmark times.
     */
    private static BenchmarkResult calculateStatistics(long[] times, long decompressedSize) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long sum = 0;
        
        for (long time : times) {
            min = Math.min(min, time);
            max = Math.max(max, time);
            sum += time;
        }
        
        double mean = (double) sum / times.length;
        
        return new BenchmarkResult(min, max, mean, decompressedSize);
    }
    
    /**
     * Print benchmark result.
     */
    private static void printResult(String name, BenchmarkResult result) {
        System.out.printf("  Min: %d ms, Max: %d ms, Mean: %.1f ms%n", 
            result.min, result.max, result.mean);
        System.out.printf("  Throughput: %.1f MB/s%n", 
            (result.decompressedSize / (1024.0 * 1024.0)) / (result.mean / 1000.0));
    }
    
    /**
     * Print summary comparison.
     */
    private static void printSummary(BenchmarkResult standard, BenchmarkResult fast, BenchmarkResult advanced) {
        System.out.println("\n=== Performance Summary ===");
        System.out.printf("Standard: %.1f ms (baseline)%n", standard.mean);
        System.out.printf("Fast:     %.1f ms (%.1fx speedup)%n", 
            fast.mean, standard.mean / fast.mean);
        System.out.printf("Advanced: %.1f ms (%.1fx speedup)%n", 
            advanced.mean, standard.mean / advanced.mean);
        
        // Find best performer
        double bestTime = Math.min(Math.min(standard.mean, fast.mean), advanced.mean);
        
        if (bestTime == fast.mean) {
            System.out.println("\n🏆 FastGzipDecompressor performed best!");
        } else if (bestTime == advanced.mean) {
            System.out.println("\n🏆 AdvancedGzipDecompressor performed best!");
        } else {
            System.out.println("\n🏆 Standard decompression performed best!");
        }
    }
    
    /**
     * Verify all output files are identical.
     */
    private static void verifyOutputs(String outputDir) throws IOException {
        System.out.println("\n=== Output Verification ===");
        
        Path outputPath = Paths.get(outputDir);
        String[] files = {"standard_output.txt.0", "fast_output.txt.0", "advanced_output.txt.0"};
        
        boolean allIdentical = true;
        
        for (int i = 1; i < files.length; i++) {
            Path file1 = outputPath.resolve(files[0]);
            Path file2 = outputPath.resolve(files[i]);
            
            if (Files.exists(file1) && Files.exists(file2)) {
                boolean identical = Files.mismatch(file1, file2) == -1;
                System.out.printf("%s vs %s: %s%n", 
                    files[0], files[i], identical ? "✅ IDENTICAL" : "❌ DIFFERENT");
                allIdentical = allIdentical && identical;
            }
        }
        
        if (allIdentical) {
            System.out.println("✅ All decompressed files are identical!");
        } else {
            System.out.println("❌ Some decompressed files differ!");
        }
    }
    
    /**
     * Format file size for display.
     */
    private static String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
    
    /**
     * Benchmark result data class.
     */
    private static class BenchmarkResult {
        final long min;
        final long max;
        final double mean;
        final long decompressedSize;
        
        BenchmarkResult(long min, long max, double mean, long decompressedSize) {
            this.min = min;
            this.max = max;
            this.mean = mean;
            this.decompressedSize = decompressedSize;
        }
    }
} 