import com.linkedin.migz.MiGzInputStream;
import com.linkedin.migz.MiGzOutputStream;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Comprehensive benchmark comparing GZIP vs MiGz decompression performance.
 * This benchmark includes the conversion time from GZIP to MiGz format.
 */
public class MiGzBenchmark {
    
    private static final int BUFFER_SIZE = 8192;
    private static final int WARMUP_RUNS = 3;
    private static final int BENCHMARK_RUNS = 5;
    
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java MiGzBenchmark <input_file>");
            System.out.println("Example: java MiGzBenchmark test_1.gz");
            return;
        }
        
        String inputFile = args[0];
        
        if (!Files.exists(Paths.get(inputFile))) {
            System.err.println("Input file does not exist: " + inputFile);
            return;
        }
        
        try {
            runBenchmark(inputFile);
        } catch (Exception e) {
            System.err.println("Benchmark failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void runBenchmark(String inputFile) throws IOException {
        System.out.println("=== MiGz vs GZIP Decompression Benchmark ===");
        System.out.println("Input file: " + inputFile);
        System.out.println("File size: " + formatFileSize(Files.size(Paths.get(inputFile))));
        System.out.println();
        
        // Warm up JVM
        System.out.println("Warming up JVM...");
        for (int i = 0; i < WARMUP_RUNS; i++) {
            decompressWithGzip(inputFile, "warmup_gzip_" + i + ".tmp");
            decompressWithMigzConversion(inputFile, "warmup_migz_" + i + ".tmp");
        }
        System.out.println("Warmup complete.\n");
        
        // Benchmark GZIP decompression
        System.out.println("Benchmarking GZIP decompression...");
        long[] gzipTimes = new long[BENCHMARK_RUNS];
        for (int i = 0; i < BENCHMARK_RUNS; i++) {
            long startTime = System.nanoTime();
            decompressWithGzip(inputFile, "benchmark_gzip_" + i + ".tmp");
            gzipTimes[i] = System.nanoTime() - startTime;
        }
        
        // Benchmark MiGz decompression (including conversion)
        System.out.println("Benchmarking MiGz decompression (including GZIP to MiGz conversion)...");
        long[] migzTimes = new long[BENCHMARK_RUNS];
        for (int i = 0; i < BENCHMARK_RUNS; i++) {
            long startTime = System.nanoTime();
            decompressWithMigzConversion(inputFile, "benchmark_migz_" + i + ".tmp");
            migzTimes[i] = System.nanoTime() - startTime;
        }
        
        // Calculate statistics
        long avgGzipTime = calculateAverage(gzipTimes);
        long avgMigzTime = calculateAverage(migzTimes);
        long minGzipTime = calculateMin(gzipTimes);
        long minMigzTime = calculateMin(migzTimes);
        
        // Print results
        printResults("GZIP Decompression", gzipTimes, avgGzipTime, minGzipTime);
        printResults("MiGz Decompression (with conversion)", migzTimes, avgMigzTime, minMigzTime);
        
        // Performance comparison
        System.out.println("\n=== Performance Comparison ===");
        double speedup = (double) avgGzipTime / avgMigzTime;
        if (speedup > 1.0) {
            System.out.printf("MiGz is %.2fx FASTER than GZIP\n", speedup);
        } else {
            System.out.printf("MiGz is %.2fx SLOWER than GZIP\n", 1.0 / speedup);
        }
        
        // Detailed breakdown
        System.out.println("\n=== Detailed Breakdown ===");
        long conversionTime = measureConversionTime(inputFile);
        long pureMigzTime = avgMigzTime - conversionTime;
        
        System.out.printf("GZIP to MiGz conversion time: %.3f ms\n", conversionTime / 1_000_000.0);
        System.out.printf("Pure MiGz decompression time: %.3f ms\n", pureMigzTime / 1_000_000.0);
        System.out.printf("Total MiGz time (conversion + decompression): %.3f ms\n", avgMigzTime / 1_000_000.0);
        System.out.printf("Pure GZIP decompression time: %.3f ms\n", avgGzipTime / 1_000_000.0);
        
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
        // Step 1: Convert GZIP to MiGz format
        String migzFile = inputFile + ".migz.tmp";
        convertGzipToMigz(inputFile, migzFile);
        
        // Step 2: Decompress MiGz file
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
    
    private static void convertGzipToMigz(String gzipFile, String migzFile) throws IOException {
        // First decompress GZIP to get original data
        byte[] originalData;
        try (FileInputStream fis = new FileInputStream(gzipFile);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            
            while ((bytesRead = gzis.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            
            originalData = baos.toByteArray();
        }
        
        // Then compress to MiGz format
        try (FileOutputStream fos = new FileOutputStream(migzFile);
             MiGzOutputStream migzOut = new MiGzOutputStream(fos)) {
            
            migzOut.write(originalData);
        }
    }
    
    private static long measureConversionTime(String inputFile) throws IOException {
        String migzFile = inputFile + ".conversion_test.tmp";
        
        long startTime = System.nanoTime();
        convertGzipToMigz(inputFile, migzFile);
        long conversionTime = System.nanoTime() - startTime;
        
        Files.deleteIfExists(Paths.get(migzFile));
        return conversionTime;
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