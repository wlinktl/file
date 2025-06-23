import com.linkedin.migz.MiGzInputStream;
import java.io.*;
import java.nio.file.*;
import java.nio.channels.*;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.concurrent.TimeUnit;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.*;
import java.text.DecimalFormat;

public class MiGzBenchmark {
    
    private static final DecimalFormat df = new DecimalFormat("#.##");
    private static final int WARMUP_ITERATIONS = 3;
    private static final int BENCHMARK_ITERATIONS = 10;
    private static final int BUFFER_SIZE = 64 * 1024; // 64KB - optimal for streaming
    private static final int LARGE_BUFFER_SIZE = 256 * 1024; // 256KB for very large files
    
    private static final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private static final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    
    public static void main(String[] args) {
        System.out.println("=== MiGZ vs Java GZIP Decompression Benchmark ===\n");
        
        // Get list of test files
        List<String> testFiles = getTestFiles();
        if (testFiles.isEmpty()) {
            System.err.println("No test files found in resources directory!");
            System.err.println("Please ensure test_1.gz, test_2.gz, etc. are in the resources directory");
            return;
        }
        
        System.out.println("Found " + testFiles.size() + " test files:");
        testFiles.forEach(file -> System.out.println("  - " + file));
        System.out.println();
        
        // Enable CPU time measurement if available
        if (!threadBean.isCurrentThreadCpuTimeSupported()) {
            System.out.println("Warning: CPU time measurement not supported on this platform");
        }
        
        // Run benchmarks for each file
        for (String testFile : testFiles) {
            runBenchmarkForFile(testFile);
            System.out.println();
        }
        
        System.out.println("=== Benchmark Complete ===");
    }
    
    private static List<String> getTestFiles() {
        List<String> testFiles = new ArrayList<>();
        File resourcesDir = new File("resources");
        
        if (!resourcesDir.exists()) {
            // Try current directory
            resourcesDir = new File(".");
        }
        
        File[] files = resourcesDir.listFiles((dir, name) -> 
            name.startsWith("test_") && name.endsWith(".gz"));
        
        if (files != null) {
            Arrays.sort(files, (a, b) -> a.getName().compareTo(b.getName()));
            for (File file : files) {
                testFiles.add(file.getPath());
            }
        }
        
        return testFiles;
    }
    
    private static void runBenchmarkForFile(String filePath) {
        System.out.println("=== Benchmarking: " + filePath + " ===");
        
        File file = new File(filePath);
        if (!file.exists()) {
            System.err.println("File not found: " + filePath);
            return;
        }
        
        long fileSize = file.length();
        System.out.println("File size: " + formatBytes(fileSize));
        
        // Warmup runs
        System.out.println("\nPerforming warmup runs...");
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            try {
                decompressWithJavaGzip(filePath, true); // Warmup, discard output
                decompressWithMiGz(filePath, true); // Warmup, discard output
            } catch (Exception e) {
                System.err.println("Warmup failed: " + e.getMessage());
                return;
            }
        }
        
        // Choose streaming approach based on file size
        boolean useNIOStreaming = fileSize > 100 * 1024 * 1024; // Use NIO for files >100MB
        
        System.out.println("Streaming approach: " + (useNIOStreaming ? "NIO Direct Buffers" : "Traditional I/O"));
        
        // Benchmark Java GZIP
        System.out.println("\nBenchmarking Java built-in GZIP decompression...");
        BenchmarkResult javaResult = benchmarkMethod(() -> {
            try {
                return useNIOStreaming ? 
                    decompressWithJavaGzipNIO(filePath, false) :
                    decompressWithJavaGzip(filePath, false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        
        // Benchmark MiGZ
        System.out.println("Benchmarking MiGZ multithreaded decompression...");
        BenchmarkResult migzResult = benchmarkMethod(() -> {
            try {
                return useNIOStreaming ?
                    decompressWithMiGzNIO(filePath, false) :
                    decompressWithMiGz(filePath, false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        
        // Print results
        printResults("Java GZIP", javaResult, fileSize);
        printResults("MiGZ", migzResult, fileSize);
        
        // Performance comparison
        printComparison(javaResult, migzResult);
    }
    
    private static long decompressWithJavaGzip(String filePath, boolean warmup) throws IOException {
        long decompressedSize = 0;
        
        // Use streaming approach with larger buffer and NIO for better performance
        try (FileInputStream fis = new FileInputStream(filePath);
             BufferedInputStream bis = new BufferedInputStream(fis, BUFFER_SIZE * 8);
             GZIPInputStream gzis = new GZIPInputStream(bis, BUFFER_SIZE)) {
            
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            
            if (warmup) {
                // For warmup, stream through data without processing
                while ((bytesRead = gzis.read(buffer)) != -1) {
                    decompressedSize += bytesRead;
                }
            } else {
                // For actual benchmark, stream and simulate real processing
                while ((bytesRead = gzis.read(buffer)) != -1) {
                    decompressedSize += bytesRead;
                    // Simulate real-world processing: streaming write or analysis
                    processDecompressedChunk(buffer, 0, bytesRead);
                }
            }
        }
        
        return decompressedSize;
    }
    
    private static long decompressWithMiGz(String filePath, boolean warmup) throws IOException {
        long decompressedSize = 0;
        
        // Use streaming approach with NIO and larger buffers
        try (FileInputStream fis = new FileInputStream(filePath);
             BufferedInputStream bis = new BufferedInputStream(fis, BUFFER_SIZE * 8);
             MiGzInputStream migzis = new MiGzInputStream(bis)) {
            
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            
            if (warmup) {
                // For warmup, stream through data without processing
                while ((bytesRead = migzis.read(buffer)) != -1) {
                    decompressedSize += bytesRead;
                }
            } else {
                // For actual benchmark, stream and simulate real processing
                while ((bytesRead = migzis.read(buffer)) != -1) {
                    decompressedSize += bytesRead;
                    // Simulate real-world processing: streaming write or analysis
                    processDecompressedChunk(buffer, 0, bytesRead);
                }
            }
        }
        
        return decompressedSize;
    }
    
    /**
     * Simulates real-world processing of decompressed data chunks
     * This could be writing to output stream, parsing, analysis, etc.
     */
    private static void processDecompressedChunk(byte[] buffer, int offset, int length) {
        // Simulate common streaming operations:
        
        // 1. Count line breaks (text processing simulation)
        int lineCount = 0;
        for (int i = offset; i < offset + length; i++) {
            if (buffer[i] == '\n') lineCount++;
        }
        
        // 2. Simple checksum calculation (integrity check simulation)
        long checksum = 0;
        for (int i = offset; i < offset + length; i++) {
            checksum += buffer[i] & 0xFF;
        }
        
        // 3. Pattern matching simulation (search operation)
        // In real scenarios, this could be regex matching, data extraction, etc.
        
        // Note: We don't store results to avoid memory accumulation,
        // but in real scenarios you'd stream results to output or accumulate statistics
    }
    
    /**
     * Enhanced decompression method for very large files using NIO streaming
     */
    private static long decompressWithJavaGzipNIO(String filePath, boolean warmup) throws IOException {
        long decompressedSize = 0;
        Path path = Paths.get(filePath);
        
        // Use NIO for better performance with large files
        try (ReadableByteChannel inputChannel = Files.newByteChannel(path, StandardOpenOption.READ);
             ReadableByteChannel gzipChannel = Channels.newChannel(
                 new GZIPInputStream(Channels.newInputStream(inputChannel), LARGE_BUFFER_SIZE))) {
            
            ByteBuffer buffer = ByteBuffer.allocateDirect(LARGE_BUFFER_SIZE);
            
            while (gzipChannel.read(buffer) != -1) {
                buffer.flip();
                int bytesRead = buffer.remaining();
                decompressedSize += bytesRead;
                
                if (!warmup) {
                    // Process the buffer content
                    processDecompressedBuffer(buffer);
                }
                
                buffer.clear();
            }
        }
        
        return decompressedSize;
    }
    
    /**
     * Enhanced MiGZ decompression with NIO for very large files
     */
    private static long decompressWithMiGzNIO(String filePath, boolean warmup) throws IOException {
        long decompressedSize = 0;
        Path path = Paths.get(filePath);
        
        try (ReadableByteChannel inputChannel = Files.newByteChannel(path, StandardOpenOption.READ);
             ReadableByteChannel migzChannel = Channels.newChannel(
                 new MiGzInputStream(Channels.newInputStream(inputChannel)))) {
            
            ByteBuffer buffer = ByteBuffer.allocateDirect(LARGE_BUFFER_SIZE);
            
            while (migzChannel.read(buffer) != -1) {
                buffer.flip();
                int bytesRead = buffer.remaining();
                decompressedSize += bytesRead;
                
                if (!warmup) {
                    // Process the buffer content
                    processDecompressedBuffer(buffer);
                }
                
                buffer.clear();
            }
        }
        
        return decompressedSize;
    }
    
    /**
     * Process decompressed data in ByteBuffer (NIO streaming)
     */
    private static void processDecompressedBuffer(ByteBuffer buffer) {
        // Simulate processing of ByteBuffer content
        int lineCount = 0;
        long checksum = 0;
        
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            if (b == '\n') lineCount++;
            checksum += b & 0xFF;
        }
        
        // Reset position for potential further processing
        buffer.rewind();
    }
        List<Long> times = new ArrayList<>();
        List<Long> memoryUsages = new ArrayList<>();
        List<Long> cpuTimes = new ArrayList<>();
        long totalDecompressedSize = 0;
        
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            // Force garbage collection before each run
            System.gc();
            try {
                Thread.sleep(100); // Give GC time to complete
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Measure memory before
            MemoryUsage memBefore = memoryBean.getHeapMemoryUsage();
            long cpuBefore = threadBean.isCurrentThreadCpuTimeSupported() ? 
                threadBean.getCurrentThreadCpuTime() : 0;
            
            // Run the decompression
            long startTime = System.nanoTime();
            long decompressedSize = task.run();
            long endTime = System.nanoTime();
            
            // Measure memory and CPU after
            MemoryUsage memAfter = memoryBean.getHeapMemoryUsage();
            long cpuAfter = threadBean.isCurrentThreadCpuTimeSupported() ? 
                threadBean.getCurrentThreadCpuTime() : 0;
            
            long duration = endTime - startTime;
            long memoryUsed = Math.max(0, memAfter.getUsed() - memBefore.getUsed());
            long cpuTime = threadBean.isCurrentThreadCpuTimeSupported() ? 
                cpuAfter - cpuBefore : 0;
            
            times.add(duration);
            memoryUsages.add(memoryUsed);
            cpuTimes.add(cpuTime);
            totalDecompressedSize = decompressedSize;
            
            System.out.print(".");
        }
        System.out.println(" Done");
        
        return new BenchmarkResult(times, memoryUsages, cpuTimes, totalDecompressedSize);
    }
    
    private static void printResults(String method, BenchmarkResult result, long originalFileSize) {
        System.out.println("\n--- " + method + " Results ---");
        System.out.println("Decompressed size: " + formatBytes(result.decompressedSize));
        System.out.println("Compression ratio: " + df.format((double) result.decompressedSize / originalFileSize) + ":1");
        
        System.out.println("\nTiming (ms):");
        System.out.println("  Average: " + df.format(result.avgTime / 1_000_000.0));
        System.out.println("  Min: " + df.format(result.minTime / 1_000_000.0));
        System.out.println("  Max: " + df.format(result.maxTime / 1_000_000.0));
        System.out.println("  Std Dev: " + df.format(result.stdDevTime / 1_000_000.0));
        
        System.out.println("\nThroughput:");
        double avgThroughputMBps = (result.decompressedSize / (1024.0 * 1024.0)) / (result.avgTime / 1_000_000_000.0);
        System.out.println("  Average: " + df.format(avgThroughputMBps) + " MB/s");
        
        System.out.println("\nMemory Usage:");
        System.out.println("  Average: " + formatBytes(result.avgMemory));
        System.out.println("  Peak: " + formatBytes(result.maxMemory));
        
        if (threadBean.isCurrentThreadCpuTimeSupported()) {
            System.out.println("\nCPU Time:");
            System.out.println("  Average: " + df.format(result.avgCpuTime / 1_000_000.0) + " ms");
            System.out.println("  CPU Efficiency: " + df.format((result.avgCpuTime / (double) result.avgTime) * 100) + "%");
        }
    }
    
    private static void printComparison(BenchmarkResult javaResult, BenchmarkResult migzResult) {
        System.out.println("\n=== Performance Comparison ===");
        
        double speedupRatio = (double) javaResult.avgTime / migzResult.avgTime;
        double memoryRatio = (double) migzResult.avgMemory / javaResult.avgMemory;
        
        System.out.println("Speed comparison:");
        if (speedupRatio > 1.0) {
            System.out.println("  MiGZ is " + df.format(speedupRatio) + "x FASTER than Java GZIP");
        } else {
            System.out.println("  MiGZ is " + df.format(1.0 / speedupRatio) + "x SLOWER than Java GZIP");
        }
        
        System.out.println("Memory comparison:");
        if (memoryRatio > 1.0) {
            System.out.println("  MiGZ uses " + df.format(memoryRatio) + "x MORE memory than Java GZIP");
        } else {
            System.out.println("  MiGZ uses " + df.format(1.0 / memoryRatio) + "x LESS memory than Java GZIP");
        }
        
        if (threadBean.isCurrentThreadCpuTimeSupported()) {
            double cpuRatio = (double) migzResult.avgCpuTime / javaResult.avgCpuTime;
            System.out.println("CPU usage comparison:");
            if (cpuRatio > 1.0) {
                System.out.println("  MiGZ uses " + df.format(cpuRatio) + "x MORE CPU time than Java GZIP");
            } else {
                System.out.println("  MiGZ uses " + df.format(1.0 / cpuRatio) + "x LESS CPU time than Java GZIP");
            }
        }
        
        // Assessment
        System.out.println("\n=== Assessment ===");
        if (speedupRatio > 1.2) { // At least 20% faster
            if (memoryRatio < 3.0) { // But not using more than 3x memory
                System.out.println("✅ RECOMMENDATION: MiGZ is WORTH using for this file size");
                System.out.println("   - Significant performance improvement with reasonable memory overhead");
            } else {
                System.out.println("⚠️  CONDITIONAL: MiGZ provides speed benefits but uses significant memory");
                System.out.println("   - Consider your memory constraints and available system resources");
            }
        } else if (speedupRatio > 0.9) { // Within 10% performance
            System.out.println("🔍 MARGINAL: Performance difference is minimal");
            System.out.println("   - Choice depends on other factors (memory, CPU cores, etc.)");
        } else {
            System.out.println("❌ NOT RECOMMENDED: Java GZIP performs better for this scenario");
            System.out.println("   - Stick with built-in Java GZIP decompression");
        }
    }
    
    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "";
        return df.format(bytes / Math.pow(1024, exp)) + " " + pre + "B";
    }
    
    @FunctionalInterface
    interface DecompressionTask {
        long run();
    }
    
    static class BenchmarkResult {
        final long avgTime, minTime, maxTime, stdDevTime;
        final long avgMemory, maxMemory;
        final long avgCpuTime;
        final long decompressedSize;
        
        BenchmarkResult(List<Long> times, List<Long> memories, List<Long> cpuTimes, long decompressedSize) {
            this.decompressedSize = decompressedSize;
            
            // Calculate timing statistics
            this.avgTime = (long) times.stream().mapToLong(Long::longValue).average().orElse(0);
            this.minTime = times.stream().mapToLong(Long::longValue).min().orElse(0);
            this.maxTime = times.stream().mapToLong(Long::longValue).max().orElse(0);
            
            double variance = times.stream()
                .mapToDouble(time -> Math.pow(time - avgTime, 2))
                .average().orElse(0);
            this.stdDevTime = (long) Math.sqrt(variance);
            
            // Calculate memory statistics
            this.avgMemory = (long) memories.stream().mapToLong(Long::longValue).average().orElse(0);
            this.maxMemory = memories.stream().mapToLong(Long::longValue).max().orElse(0);
            
            // Calculate CPU statistics
            this.avgCpuTime = (long) cpuTimes.stream().mapToLong(Long::longValue).average().orElse(0);
        }
    }
}