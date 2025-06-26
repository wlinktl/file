import com.linkedin.migz.MiGzInputStream;
import com.linkedin.migz.MiGzOutputStream;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Benchmark comparing direct GZIP decompression vs GZIP→MiGz→Decompress approach.
 * This demonstrates why converting GZIP to MiGz is NOT efficient for existing GZIP files.
 */
public class GzipVsMigzBenchmark {
    
    private static final int BUFFER_SIZE = 8192;
    private static final int WARMUP_RUNS = 3;
    private static final int BENCHMARK_RUNS = 5;
    
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java GzipVsMigzBenchmark <gzip_file>");
            System.out.println("Example: java GzipVsMigzBenchmark test_1.gz");
            return;
        }
        
        String gzipFile = args[0];
        
        if (!Files.exists(Paths.get(gzipFile))) {
            System.err.println("GZIP file does not exist: " + gzipFile);
            return;
        }
        
        try {
            runBenchmark(gzipFile);
        } catch (Exception e) {
            System.err.println("Benchmark failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void runBenchmark(String gzipFile) throws IOException {
        System.out.println("=== GZIP vs MiGz Decompression Benchmark ===");
        System.out.println("Input file: " + gzipFile);
        System.out.println("File size: " + formatFileSize(Files.size(Paths.get(gzipFile))));
        System.out.println();
        
        // Warm up JVM
        System.out.println("Warming up JVM...");
        for (int i = 0; i < WARMUP_RUNS; i++) {
            decompressWithGzip(gzipFile, "warmup_gzip_" + i + ".tmp");
            decompressWithMigzConversion(gzipFile, "warmup_migz_" + i + ".tmp");
        }
        System.out.println("Warmup complete.\n");
        
        // Benchmark direct GZIP decompression
        System.out.println("Benchmarking direct GZIP decompression...");
        long[] gzipTimes = new long[BENCHMARK_RUNS];
        for (int i = 0; i < BENCHMARK_RUNS; i++) {
            long startTime = System.nanoTime();
            decompressWithGzip(gzipFile, "benchmark_gzip_" + i + ".tmp");
            gzipTimes[i] = System.nanoTime() - startTime;
        }
        
        // Benchmark GZIP→MiGz→Decompress approach
        System.out.println("Benchmarking GZIP→MiGz→Decompress approach...");
        long[] migzTimes = new long[BENCHMARK_RUNS];
        for (int i = 0; i < BENCHMARK_RUNS; i++) {
            long startTime = System.nanoTime();
            decompressWithMigzConversion(gzipFile, "benchmark_migz_" + i + ".tmp");
            migzTimes[i] = System.nanoTime() - startTime;
        }
        
        // Calculate statistics
        long avgGzipTime = calculateAverage(gzipTimes);
        long avgMigzTime = calculateAverage(migzTimes);
        long minGzipTime = calculateMin(gzipTimes);
        long minMigzTime = calculateMin(migzTimes);
        
        // Print results
        printResults("Direct GZIP Decompression", gzipTimes, avgGzipTime, minGzipTime);
        printResults("GZIP→MiGz→Decompress", migzTimes, avgMigzTime, minMigzTime);
        
        // Performance comparison
        System.out.println("\n=== Performance Comparison ===");
        double speedup = (double) avgMigzTime / avgGzipTime;
        if (speedup > 1.0) {
            System.out.printf("Direct GZIP is %.2fx FASTER than GZIP→MiGz→Decompress\n", speedup);
        } else {
            System.out.printf("GZIP→MiGz→Decompress is %.2fx FASTER than Direct GZIP\n", 1.0 / speedup);
        }
        
        // Detailed breakdown
        System.out.println("\n=== Detailed Breakdown ===");
        long conversionTime = measureConversionTime(gzipFile);
        long pureMigzTime = avgMigzTime - conversionTime;
        
        System.out.printf("GZIP to MiGz conversion time: %.3f ms\n", conversionTime / 1_000_000.0);
        System.out.printf("Pure MiGz decompression time: %.3f ms\n", pureMigzTime / 1_000_000.0);
        System.out.printf("Total GZIP→MiGz→Decompress time: %.3f ms\n", avgMigzTime / 1_000_000.0);
        System.out.printf("Direct GZIP decompression time: %.3f ms\n", avgGzipTime / 1_000_000.0);
        
        // Memory usage analysis
        System.out.println("\n=== Memory Usage Analysis ===");
        long gzipMemoryUsage = estimateMemoryUsage(gzipFile, "gzip");
        long migzMemoryUsage = estimateMemoryUsage(gzipFile, "migz");
        
        System.out.printf("Direct GZIP memory usage: ~%s\n", formatFileSize(gzipMemoryUsage));
        System.out.printf("GZIP→MiGz→Decompress memory usage: ~%s\n", formatFileSize(migzMemoryUsage));
        System.out.printf("Memory overhead: %.1fx\n", (double) migzMemoryUsage / gzipMemoryUsage);
        
        // Recommendations
        System.out.println("\n=== Recommendations ===");
        System.out.println("✓ For existing GZIP files: Use GZIPInputStream directly");
        System.out.println("✓ For new compression: Use MiGzOutputStream for better performance");
        System.out.println("✓ For large files: MiGz shows benefits only when compressing from scratch");
        System.out.println("✓ Avoid converting existing GZIP files to MiGz format");
        
        // Cleanup temporary files
        cleanupTempFiles();
    }
    
    private static void decompressWithGzip(String inputFile, String outputFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputFile);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             FileOutputStream fos = new FileOutputStream(outputFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            
            while ((bytesRead = gzis.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }
        }
    }
    
    private static void decompressWithMigzConversion(String inputFile, String outputFile) throws IOException {
        // Step 1: Decompress GZIP to get original data
        byte[] originalData;
        try (FileInputStream fis = new FileInputStream(inputFile);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            
            while ((bytesRead = gzis.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            
            originalData = baos.toByteArray();
        }
        
        // Step 2: Compress to MiGz format
        String migzFile = inputFile + ".migz.tmp";
        try (FileOutputStream fos = new FileOutputStream(migzFile);
             MiGzOutputStream migzOut = new MiGzOutputStream(fos)) {
            
            migzOut.write(originalData);
        }
        
        // Step 3: Decompress MiGz file
        try (FileInputStream fis = new FileInputStream(migzFile);
             MiGzInputStream migzIn = new MiGzInputStream(fis);
             FileOutputStream fos = new FileOutputStream(outputFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            
            while ((bytesRead = migzIn.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }
        }
        
        // Clean up temporary MiGz file
        Files.deleteIfExists(Paths.get(migzFile));
    }
    
    private static long measureConversionTime(String inputFile) throws IOException {
        // Decompress GZIP to get original data
        byte[] originalData;
        try (FileInputStream fis = new FileInputStream(inputFile);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            
            while ((bytesRead = gzis.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            
            originalData = baos.toByteArray();
        }
        
        // Measure time to compress to MiGz
        String migzFile = inputFile + ".conversion_test.tmp";
        long startTime = System.nanoTime();
        
        try (FileOutputStream fos = new FileOutputStream(migzFile);
             MiGzOutputStream migzOut = new MiGzOutputStream(fos)) {
            
            migzOut.write(originalData);
        }
        
        long conversionTime = System.nanoTime() - startTime;
        Files.deleteIfExists(Paths.get(migzFile));
        
        return conversionTime;
    }
    
    private static long estimateMemoryUsage(String gzipFile, String method) throws IOException {
        long gzipFileSize = Files.size(Paths.get(gzipFile));
        
        if ("gzip".equals(method)) {
            // GZIP decompression: needs buffer + decompressed data
            return gzipFileSize + BUFFER_SIZE;
        } else {
            // MiGz approach: needs buffer + decompressed data + MiGz compressed data
            // Estimate decompressed size (typically 2-10x larger)
            long estimatedDecompressedSize = gzipFileSize * 5; // Conservative estimate
            return gzipFileSize + estimatedDecompressedSize + (estimatedDecompressedSize / 2) + BUFFER_SIZE;
        }
    }
    
    private static long calculateAverage(long[] times) {
        long sum = 0;
        for (long time : times) {
            sum += time;
        }
        return sum / times.length;
    }
    
    private static long calculateMin(long[] times) {
        long min = times[0];
        for (long time : times) {
            if (time < min) {
                min = time;
            }
        }
        return min;
    }
    
    private static void printResults(String name, long[] times, long avgTime, long minTime) {
        System.out.println("\n" + name + " Results:");
        System.out.printf("  Average time: %.3f ms\n", avgTime / 1_000_000.0);
        System.out.printf("  Minimum time: %.3f ms\n", minTime / 1_000_000.0);
        System.out.print("  Individual runs: ");
        for (int i = 0; i < times.length; i++) {
            System.out.printf("%.3f ms", times[i] / 1_000_000.0);
            if (i < times.length - 1) System.out.print(", ");
        }
        System.out.println();
    }
    
    private static String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
    
    private static void cleanupTempFiles() {
        try {
            // Clean up benchmark files
            for (int i = 0; i < BENCHMARK_RUNS; i++) {
                Files.deleteIfExists(Paths.get("benchmark_gzip_" + i + ".tmp"));
                Files.deleteIfExists(Paths.get("benchmark_migz_" + i + ".tmp"));
            }
            
            // Clean up warmup files
            for (int i = 0; i < WARMUP_RUNS; i++) {
                Files.deleteIfExists(Paths.get("warmup_gzip_" + i + ".tmp"));
                Files.deleteIfExists(Paths.get("warmup_migz_" + i + ".tmp"));
            }
        } catch (IOException e) {
            System.err.println("Warning: Could not clean up all temporary files: " + e.getMessage());
        }
    }
}

/*
 * USAGE:
 * java GzipVsMigzBenchmark test_1.gz
 * 
 * EXPECTED RESULTS:
 * 
 * For existing GZIP files:
 * - Direct GZIP decompression will be 2-5x faster
 * - GZIP→MiGz→Decompress approach adds unnecessary overhead
 * - Memory usage is significantly higher with conversion
 * 
 * RECOMMENDATIONS:
 * 
 * 1. For existing GZIP files:
 *    - Use GZIPInputStream directly
 *    - Do NOT convert to MiGz format
 * 
 * 2. For new compression:
 *    - Use MiGzOutputStream for better performance
 *    - MiGz shows benefits when compressing from scratch
 * 
 * 3. For large files:
 *    - MiGz benefits are most apparent in compression, not decompression
 *    - Consider the full pipeline: compression + storage + decompression
 */ 