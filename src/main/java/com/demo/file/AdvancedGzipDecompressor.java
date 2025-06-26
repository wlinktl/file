package com.demo.file;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.DataFormatException;

/**
 * Advanced multi-threaded GZIP decompressor for very large files.
 * Uses memory-mapped files and streaming approaches for optimal performance.
 */
public class AdvancedGzipDecompressor implements AutoCloseable {
    
    private static final int BUFFER_SIZE = 128 * 1024; // 128KB buffer
    private static final int DEFAULT_THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final long MAX_MEMORY_MAPPED_SIZE = 2L * 1024 * 1024 * 1024; // 2GB
    
    private final int threadCount;
    private final ExecutorService executor;
    
    public AdvancedGzipDecompressor() {
        this(DEFAULT_THREAD_COUNT);
    }
    
    public AdvancedGzipDecompressor(int threadCount) {
        this.threadCount = threadCount;
        this.executor = Executors.newFixedThreadPool(threadCount);
    }
    
    /**
     * Decompress a GZIP file using the best strategy based on file size.
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
        
        // Choose strategy based on file size
        if (fileSize <= MAX_MEMORY_MAPPED_SIZE) {
            System.out.println("Using memory-mapped strategy...");
            decompressWithMemoryMapping(inputPath, outputPath);
        } else {
            System.out.println("Using streaming strategy...");
            decompressWithStreaming(inputPath, outputPath);
        }
        
        long endTime = System.currentTimeMillis();
        long decompressedSize = Files.size(outputPath);
        
        System.out.println("Decompression completed in " + (endTime - startTime) + " ms");
        System.out.println("Decompressed size: " + formatFileSize(decompressedSize));
        System.out.println("Throughput: " + formatFileSize(decompressedSize / Math.max(1, (endTime - startTime) / 1000)) + "/s");
    }
    
    /**
     * Decompress using memory-mapped files for better performance.
     */
    private void decompressWithMemoryMapping(Path inputPath, Path outputPath) throws IOException {
        try (FileChannel inputChannel = FileChannel.open(inputPath, java.nio.file.StandardOpenOption.READ);
             FileOutputStream fos = new FileOutputStream(outputPath.toFile());
             BufferedOutputStream bos = new BufferedOutputStream(fos, BUFFER_SIZE)) {
            
            long fileSize = Files.size(inputPath);
            MappedByteBuffer buffer = inputChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
            
            // Read GZIP header
            byte[] header = readGzipHeaderFromBuffer(buffer);
            if (header == null) {
                throw new IOException("Invalid GZIP file: missing or invalid header");
            }
            
            // Skip header
            buffer.position(header.length);
            
            // Decompress the rest
            decompressBufferToStream(buffer, bos);
        }
    }
    
    /**
     * Decompress using streaming approach for large files.
     */
    private void decompressWithStreaming(Path inputPath, Path outputPath) throws IOException {
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
                
                // Progress indicator
                if (totalBytes % (50 * 1024 * 1024) == 0) { // Every 50MB
                    System.out.println("Processed: " + formatFileSize(totalBytes));
                }
            }
            
            bos.flush();
        }
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
     * Read GZIP header from buffer.
     */
    private byte[] readGzipHeaderFromBuffer(MappedByteBuffer buffer) {
        if (buffer.remaining() < 10) {
            return null;
        }
        
        byte[] header = new byte[10];
        buffer.get(header);
        
        // Check GZIP magic bytes
        if (header[0] != (byte) 0x1f || header[1] != (byte) 0x8b) {
            return null;
        }
        
        // Check compression method (should be 8 for deflate)
        if (header[2] != 8) {
            return null;
        }
        
        return header;
    }
    
    /**
     * Decompress buffer to output stream.
     */
    private void decompressBufferToStream(MappedByteBuffer buffer, BufferedOutputStream output) throws IOException {
        Inflater inflater = new Inflater(true); // true for GZIP format
        byte[] inputBuffer = new byte[BUFFER_SIZE];
        byte[] outputBuffer = new byte[BUFFER_SIZE];
        
        try {
            while (buffer.hasRemaining()) {
                int bytesToRead = Math.min(buffer.remaining(), inputBuffer.length);
                buffer.get(inputBuffer, 0, bytesToRead);
                
                inflater.setInput(inputBuffer, 0, bytesToRead);
                
                int bytesDecompressed;
                try {
                    while ((bytesDecompressed = inflater.inflate(outputBuffer)) > 0) {
                        output.write(outputBuffer, 0, bytesDecompressed);
                    }
                } catch (DataFormatException e) {
                    throw new IOException("Error decompressing data", e);
                }
                
                if (inflater.needsInput() && !buffer.hasRemaining()) {
                    break;
                }
            }
            
            // Handle any remaining data
            if (!inflater.finished()) {
                inflater.setInput(new byte[0]);
                int bytesDecompressed;
                try {
                    while ((bytesDecompressed = inflater.inflate(outputBuffer)) > 0) {
                        output.write(outputBuffer, 0, bytesDecompressed);
                    }
                } catch (DataFormatException e) {
                    throw new IOException("Error decompressing remaining data", e);
                }
            }
            
        } finally {
            inflater.end();
        }
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
        System.out.println("=== Advanced GZIP Decompression Benchmark ===");
        
        // Test 1: Advanced decompression
        long startTime = System.currentTimeMillis();
        String output1 = inputFile + ".advanced1";
        decompressFile(inputFile, output1);
        long time1 = System.currentTimeMillis() - startTime;
        
        // Test 2: Parallel processing
        startTime = System.currentTimeMillis();
        String output2 = inputFile + ".advanced2";
        decompressWithParallelProcessing(inputFile, output2);
        long time2 = System.currentTimeMillis() - startTime;
        
        // Test 3: Standard decompression for comparison
        startTime = System.currentTimeMillis();
        String output3 = inputFile + ".advanced3";
        decompressStandard(inputFile, output3);
        long time3 = System.currentTimeMillis() - startTime;
        
        // Results
        System.out.println("\n=== Benchmark Results ===");
        System.out.println("Advanced decompression: " + time1 + " ms");
        System.out.println("Parallel processing: " + time2 + " ms");
        System.out.println("Standard decompression: " + time3 + " ms");
        
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
     * Standard decompression for comparison.
     */
    private void decompressStandard(String inputFile, String outputFile) throws IOException {
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
            System.out.println("Usage: java AdvancedGzipDecompressor <input.gz> <output> [threads] [mode]");
            System.out.println("Modes: auto, parallel, benchmark");
            System.out.println("Example: java AdvancedGzipDecompressor large_file.gz output.txt 8 benchmark");
            return;
        }
        
        String inputFile = args[0];
        String outputFile = args[1];
        int threads = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_THREAD_COUNT;
        String mode = args.length > 3 ? args[3] : "auto";
        
        try (AdvancedGzipDecompressor decompressor = new AdvancedGzipDecompressor(threads)) {
            
            switch (mode.toLowerCase()) {
                case "auto":
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