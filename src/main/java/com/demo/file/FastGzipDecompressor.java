package com.demo.file;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;

/**
 * Fast GZIP decompressor with optimized performance for large files.
 * Uses buffered I/O and optional parallel post-processing.
 */
public class FastGzipDecompressor implements AutoCloseable {
    
    private static final int BUFFER_SIZE = 64 * 1024; // 64KB buffer
    private static final int DEFAULT_THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    
    private final int threadCount;
    private final ExecutorService executor;
    
    public FastGzipDecompressor() {
        this(DEFAULT_THREAD_COUNT);
    }
    
    public FastGzipDecompressor(int threadCount) {
        this.threadCount = threadCount;
        this.executor = Executors.newFixedThreadPool(threadCount);
    }
    
    /**
     * Decompress a GZIP file with optimized performance.
     */
    public void decompressFile(String inputFile, String outputFile) throws IOException {
        Path inputPath = Paths.get(inputFile);
        Path outputPath = Paths.get(outputFile);
        
        if (!Files.exists(inputPath)) {
            throw new FileNotFoundException("Input file not found: " + inputFile);
        }
        
        long fileSize = Files.size(inputPath);
        System.out.println("Decompressing: " + inputFile);
        System.out.println("File size: " + formatFileSize(fileSize));
        System.out.println("Using " + threadCount + " threads");
        
        long startTime = System.currentTimeMillis();
        
        // Use buffered I/O for optimal performance
        try (FileInputStream fis = new FileInputStream(inputPath.toFile());
             BufferedInputStream bis = new BufferedInputStream(fis, BUFFER_SIZE);
             GZIPInputStream gzis = new GZIPInputStream(bis, BUFFER_SIZE);
             FileOutputStream fos = new FileOutputStream(outputPath.toFile());
             BufferedOutputStream bos = new BufferedOutputStream(fos, BUFFER_SIZE)) {
            
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            long totalBytes = 0;
            
            while ((bytesRead = gzis.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
                totalBytes += bytesRead;
                
                // Progress indicator for large files
                if (totalBytes % (10 * 1024 * 1024) == 0) { // Every 10MB
                    System.out.println("Processed: " + formatFileSize(totalBytes));
                }
            }
            
            bos.flush();
        }
        
        long endTime = System.currentTimeMillis();
        long decompressedSize = Files.size(outputPath);
        
        System.out.println("Decompression completed in " + (endTime - startTime) + " ms");
        System.out.println("Decompressed size: " + formatFileSize(decompressedSize));
        System.out.println("Throughput: " + formatFileSize(decompressedSize / Math.max(1, (endTime - startTime) / 1000)) + "/s");
    }
    
    /**
     * Decompress with parallel post-processing for text files.
     */
    public void decompressWithParallelProcessing(String inputFile, String outputFile) throws IOException {
        System.out.println("Decompressing with parallel post-processing...");
        long startTime = System.currentTimeMillis();
        
        // First, decompress to memory
        byte[] decompressedData = decompressToByteArray(inputFile);
        
        // Then process in parallel
        processInParallel(decompressedData, outputFile);
        
        long endTime = System.currentTimeMillis();
        System.out.println("Parallel processing completed in " + (endTime - startTime) + " ms");
    }
    
    /**
     * Decompress to byte array.
     */
    public byte[] decompressToByteArray(String inputFile) throws IOException {
        Path inputPath = Paths.get(inputFile);
        
        try (FileInputStream fis = new FileInputStream(inputPath.toFile());
             BufferedInputStream bis = new BufferedInputStream(fis, BUFFER_SIZE);
             GZIPInputStream gzis = new GZIPInputStream(bis, BUFFER_SIZE);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            
            while ((bytesRead = gzis.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            
            return baos.toByteArray();
        }
    }
    
    /**
     * Process decompressed data in parallel.
     */
    private void processInParallel(byte[] data, String outputFile) throws IOException {
        int chunkSize = Math.max(1024 * 1024, data.length / threadCount); // 1MB minimum
        int numChunks = (int) Math.ceil((double) data.length / chunkSize);
        
        System.out.println("Processing " + numChunks + " chunks in parallel...");
        
        Future<byte[]>[] futures = new Future[numChunks];
        
        for (int i = 0; i < numChunks; i++) {
            final int chunkIndex = i;
            final int start = i * chunkSize;
            final int end = Math.min(start + chunkSize, data.length);
            
            futures[chunkIndex] = executor.submit(() -> 
                processChunk(data, start, end, chunkIndex));
        }
        
        // Write results in order
        try (FileOutputStream fos = new FileOutputStream(outputFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos, BUFFER_SIZE)) {
            
            for (int i = 0; i < numChunks; i++) {
                try {
                    byte[] processedChunk = futures[i].get();
                    bos.write(processedChunk);
                } catch (InterruptedException | ExecutionException e) {
                    throw new IOException("Error processing chunk " + i, e);
                }
            }
        }
    }
    
    /**
     * Process a chunk of data.
     */
    private byte[] processChunk(byte[] data, int start, int end, int chunkIndex) {
        // For now, just return the data as-is
        // You can add custom processing here (filtering, transformation, etc.)
        
        byte[] chunk = new byte[end - start];
        System.arraycopy(data, start, chunk, 0, chunk.length);
        
        // Example: Count lines
        int lineCount = 0;
        for (byte b : chunk) {
            if (b == '\n') {
                lineCount++;
            }
        }
        
        if (chunkIndex % 10 == 0) {
            System.out.println("Chunk " + chunkIndex + " processed " + lineCount + " lines");
        }
        
        return chunk;
    }
    
    /**
     * Decompress to string.
     */
    public String decompressToString(String inputFile, String charset) throws IOException {
        byte[] data = decompressToByteArray(inputFile);
        return new String(data, charset);
    }
    
    /**
     * Benchmark performance.
     */
    public void benchmark(String inputFile) throws IOException {
        System.out.println("=== GZIP Decompression Benchmark ===");
        
        // Test 1: Optimized decompression
        long startTime = System.currentTimeMillis();
        String output1 = inputFile + ".benchmark1";
        decompressFile(inputFile, output1);
        long time1 = System.currentTimeMillis() - startTime;
        
        // Test 2: Parallel processing
        startTime = System.currentTimeMillis();
        String output2 = inputFile + ".benchmark2";
        decompressWithParallelProcessing(inputFile, output2);
        long time2 = System.currentTimeMillis() - startTime;
        
        // Test 3: Single-threaded for comparison
        startTime = System.currentTimeMillis();
        String output3 = inputFile + ".benchmark3";
        decompressSingleThreaded(inputFile, output3);
        long time3 = System.currentTimeMillis() - startTime;
        
        // Results
        System.out.println("\n=== Benchmark Results ===");
        System.out.println("Optimized decompression: " + time1 + " ms");
        System.out.println("Parallel processing: " + time2 + " ms");
        System.out.println("Single-threaded: " + time3 + " ms");
        
        // Verify files are identical
        boolean identical1 = Files.mismatch(Paths.get(output1), Paths.get(output2)) == -1;
        boolean identical2 = Files.mismatch(Paths.get(output1), Paths.get(output3)) == -1;
        
        System.out.println("Files identical (1&2): " + identical1);
        System.out.println("Files identical (1&3): " + identical2);
        
        // Cleanup
        Files.deleteIfExists(Paths.get(output1));
        Files.deleteIfExists(Paths.get(output2));
        Files.deleteIfExists(Paths.get(output3));
    }
    
    /**
     * Single-threaded decompression for comparison.
     */
    private void decompressSingleThreaded(String inputFile, String outputFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputFile);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             FileOutputStream fos = new FileOutputStream(outputFile)) {
            
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            
            while ((bytesRead = gzis.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }
    }
    
    /**
     * Format file size for display.
     */
    private String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
    
    /**
     * Close the executor service.
     */
    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Main method for command-line usage.
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java FastGzipDecompressor <input.gz> <output> [threads] [mode]");
            System.out.println("Modes: standard, parallel, benchmark");
            System.out.println("Example: java FastGzipDecompressor large_file.gz output.txt 8 benchmark");
            return;
        }
        
        String inputFile = args[0];
        String outputFile = args[1];
        int threads = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_THREAD_COUNT;
        String mode = args.length > 3 ? args[3] : "standard";
        
        try (FastGzipDecompressor decompressor = new FastGzipDecompressor(threads)) {
            
            switch (mode.toLowerCase()) {
                case "standard":
                    decompressor.decompressFile(inputFile, outputFile);
                    break;
                    
                case "parallel":
                    decompressor.decompressWithParallelProcessing(inputFile, outputFile);
                    break;
                    
                case "benchmark":
                    decompressor.benchmark(inputFile);
                    break;
                    
                default:
                    System.err.println("Unknown mode: " + mode);
                    return;
            }
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

/*
 * USAGE EXAMPLES:
 * 
 * 1. Standard decompression:
 *    FastGzipDecompressor decompressor = new FastGzipDecompressor();
 *    decompressor.decompressFile("large_file.gz", "output.txt");
 * 
 * 2. Parallel processing:
 *    FastGzipDecompressor decompressor = new FastGzipDecompressor(8);
 *    decompressor.decompressWithParallelProcessing("large_file.gz", "output.txt");
 * 
 * 3. Benchmark:
 *    FastGzipDecompressor decompressor = new FastGzipDecompressor();
 *    decompressor.benchmark("large_file.gz");
 * 
 * 4. Command line:
 *    java FastGzipDecompressor large_file.gz output.txt 8 standard
 *    java FastGzipDecompressor large_file.gz output.txt 8 parallel
 *    java FastGzipDecompressor large_file.gz output.txt 8 benchmark
 * 
 * PERFORMANCE TIPS:
 * 
 * 1. Use larger buffer sizes for better I/O performance
 * 2. Use parallel processing for text files that need post-processing
 * 3. Consider memory-mapped files for very large files
 * 4. Use SSD storage for better I/O performance
 * 5. Adjust thread count based on your system's capabilities
 * 
 * LIMITATIONS:
 * 
 * 1. GZIP format requires sequential decompression
 * 2. Parallel processing is limited to post-decompression operations
 * 3. Memory usage scales with file size for parallel processing
 * 4. Not suitable for streaming applications
 */ 