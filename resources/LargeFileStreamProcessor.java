import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.stream.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.time.*;
import java.util.zip.*;

/**
 * Demonstrates various streaming approaches for processing very large files (>2GB)
 * Each approach is optimized for different scenarios and file types.
 */
public class LargeFileStreamProcessor {
    
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024; // 64KB
    private static final int LARGE_BUFFER_SIZE = 1024 * 1024; // 1MB
    private static final int MAP_SIZE = 64 * 1024 * 1024; // 64MB mapping chunks
    
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java LargeFileStreamProcessor <file-path> [operation]");
            System.out.println("Operations: text-lines, binary-chunks, memory-mapped, compressed, parallel");
            return;
        }
        
        String filePath = args[0];
        String operation = args.length > 1 ? args[1] : "auto";
        
        Path path = Paths.get(filePath);
        if (!Files.exists(path)) {
            System.err.println("File not found: " + filePath);
            return;
        }
        
        try {
            long fileSize = Files.size(path);
            System.out.println("Processing file: " + filePath);
            System.out.println("File size: " + formatBytes(fileSize));
            System.out.println("Operation: " + operation);
            System.out.println();
            
            switch (operation.toLowerCase()) {
                case "text-lines":
                    processTextLinesStreaming(path);
                    break;
                case "binary-chunks":
                    processBinaryChunks(path);
                    break;
                case "memory-mapped":
                    processMemoryMapped(path);
                    break;
                case "compressed":
                    processCompressedFile(path);
                    break;
                case "parallel":
                    processParallelStreaming(path);
                    break;
                case "auto":
                default:
                    autoDetectAndProcess(path);
                    break;
            }
            
        } catch (IOException e) {
            System.err.println("Error processing file: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Auto-detect file type and choose appropriate processing method
     */
    private static void autoDetectAndProcess(Path path) throws IOException {
        String fileName = path.getFileName().toString().toLowerCase();
        long fileSize = Files.size(path);
        
        if (fileName.endsWith(".gz") || fileName.endsWith(".gzip")) {
            System.out.println("Detected compressed file - using compressed streaming");
            processCompressedFile(path);
        } else if (isTextFile(path)) {
            System.out.println("Detected text file - using line streaming");
            processTextLinesStreaming(path);
        } else if (fileSize > 100 * 1024 * 1024) { // >100MB
            System.out.println("Large binary file - using memory mapping");
            processMemoryMapped(path);
        } else {
            System.out.println("Binary file - using chunk processing");
            processBinaryChunks(path);
        }
    }
    
    /**
     * Stream processing for text files using Files.lines()
     * Most efficient for line-based text processing
     */
    private static void processTextLinesStreaming(Path path) throws IOException {
        System.out.println("=== Text Lines Streaming Processing ===");
        
        long startTime = System.nanoTime();
        long lineCount = 0;
        long totalChars = 0;
        long maxLineLength = 0;
        
        try (Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
            // Process lines in streaming fashion
            StreamSummary summary = lines
                .peek(line -> System.out.print(".")) // Progress indicator
                .mapToLong(line -> {
                    // Your processing logic here
                    return line.length();
                })
                .collect(
                    () -> new StreamSummary(),
                    (sum, length) -> {
                        sum.count++;
                        sum.totalLength += length;
                        sum.maxLength = Math.max(sum.maxLength, length);
                    },
                    (sum1, sum2) -> {
                        sum1.count += sum2.count;
                        sum1.totalLength += sum2.totalLength;
                        sum1.maxLength = Math.max(sum1.maxLength, sum2.maxLength);
                        return sum1;
                    }
                );
                
            long duration = System.nanoTime() - startTime;
            
            System.out.println("\n\nResults:");
            System.out.println("Lines processed: " + summary.count);
            System.out.println("Total characters: " + summary.totalLength);
            System.out.println("Average line length: " + (summary.totalLength / summary.count));
            System.out.println("Max line length: " + summary.maxLength);
            System.out.println("Processing time: " + (duration / 1_000_000) + " ms");
            System.out.println("Throughput: " + formatBytes((long)(summary.totalLength / (duration / 1e9))) + "/s");
        }
    }
    
    /**
     * Chunk-based processing for binary files
     * Good for general binary file processing
     */
    private static void processBinaryChunks(Path path) throws IOException {
        System.out.println("=== Binary Chunk Processing ===");
        
        long startTime = System.nanoTime();
        long bytesProcessed = 0;
        int chunkCount = 0;
        
        try (BufferedInputStream bis = new BufferedInputStream(
                Files.newInputStream(path), LARGE_BUFFER_SIZE)) {
            
            byte[] buffer = new byte[LARGE_BUFFER_SIZE];
            int bytesRead;
            
            while ((bytesRead = bis.read(buffer)) != -1) {
                // Process chunk
                processChunk(buffer, 0, bytesRead);
                
                bytesProcessed += bytesRead;
                chunkCount++;
                
                // Progress indicator
                if (chunkCount % 100 == 0) {
                    System.out.print(".");
                }
            }
        }
        
        long duration = System.nanoTime() - startTime;
        
        System.out.println("\n\nResults:");
        System.out.println("Bytes processed: " + formatBytes(bytesProcessed));
        System.out.println("Chunks processed: " + chunkCount);
        System.out.println("Average chunk size: " + formatBytes(bytesProcessed / chunkCount));
        System.out.println("Processing time: " + (duration / 1_000_000) + " ms");
        System.out.println("Throughput: " + formatBytes((long)(bytesProcessed / (duration / 1e9))) + "/s");
    }
    
    /**
     * Memory-mapped file processing
     * Best for large binary files and random access patterns
     */
    private static void processMemoryMapped(Path path) throws IOException {
        System.out.println("=== Memory-Mapped File Processing ===");
        
        long startTime = System.nanoTime();
        long bytesProcessed = 0;
        int mappingCount = 0;
        
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r");
             FileChannel channel = raf.getChannel()) {
            
            long fileSize = channel.size();
            long position = 0;
            
            while (position < fileSize) {
                // Calculate mapping size (handle end of file)
                long remaining = fileSize - position;
                int mapSize = (int) Math.min(MAP_SIZE, remaining);
                
                // Map portion of file
                MappedByteBuffer buffer = channel.map(
                    FileChannel.MapMode.READ_ONLY, position, mapSize);
                
                // Process mapped buffer
                processMappedBuffer(buffer);
                
                bytesProcessed += mapSize;
                position += mapSize;
                mappingCount++;
                
                // Progress indicator
                System.out.print(".");
                
                // Important: unmap buffer to prevent memory leaks
                // Note: This uses sun.misc.Unsafe in real implementations
                buffer = null;
                System.gc(); // Force GC to unmap
            }
        }
        
        long duration = System.nanoTime() - startTime;
        
        System.out.println("\n\nResults:");
        System.out.println("Bytes processed: " + formatBytes(bytesProcessed));
        System.out.println("Memory mappings: " + mappingCount);
        System.out.println("Average mapping size: " + formatBytes(bytesProcessed / mappingCount));
        System.out.println("Processing time: " + (duration / 1_000_000) + " ms");
        System.out.println("Throughput: " + formatBytes((long)(bytesProcessed / (duration / 1e9))) + "/s");
    }
    
    /**
     * Compressed file streaming processing
     * Handles .gz, .gzip files efficiently
     */
    private static void processCompressedFile(Path path) throws IOException {
        System.out.println("=== Compressed File Streaming Processing ===");
        
        long startTime = System.nanoTime();
        long bytesProcessed = 0;
        int chunkCount = 0;
        
        try (GZIPInputStream gis = new GZIPInputStream(
                new BufferedInputStream(Files.newInputStream(path), LARGE_BUFFER_SIZE),
                LARGE_BUFFER_SIZE)) {
            
            byte[] buffer = new byte[LARGE_BUFFER_SIZE];
            int bytesRead;
            
            while ((bytesRead = gis.read(buffer)) != -1) {
                // Process decompressed chunk
                processChunk(buffer, 0, bytesRead);
                
                bytesProcessed += bytesRead;
                chunkCount++;
                
                if (chunkCount % 50 == 0) {
                    System.out.print(".");
                }
            }
        }
        
        long duration = System.nanoTime() - startTime;
        long compressedSize = Files.size(path);
        
        System.out.println("\n\nResults:");
        System.out.println("Compressed file size: " + formatBytes(compressedSize));
        System.out.println("Decompressed bytes: " + formatBytes(bytesProcessed));
        System.out.println("Compression ratio: " + String.format("%.2f:1", 
            (double)bytesProcessed / compressedSize));
        System.out.println("Chunks processed: " + chunkCount);
        System.out.println("Processing time: " + (duration / 1_000_000) + " ms");
        System.out.println("Decompression throughput: " + 
            formatBytes((long)(bytesProcessed / (duration / 1e9))) + "/s");
    }
    
    /**
     * Parallel streaming processing for text files
     * Uses parallel streams for CPU-intensive processing
     */
    private static void processParallelStreaming(Path path) throws IOException {
        System.out.println("=== Parallel Streaming Processing ===");
        
        long startTime = System.nanoTime();
        
        try (Stream<String> lines = Files.lines(path)) {
            ProcessingSummary summary = lines
                .parallel() // Enable parallel processing
                .filter(line -> !line.trim().isEmpty()) // Filter empty lines
                .map(line -> {
                    // CPU-intensive processing simulation
                    return processLineIntensively(line);
                })
                .collect(
                    ProcessingSummary::new,
                    ProcessingSummary::accumulate,
                    ProcessingSummary::combine
                );
                
            long duration = System.nanoTime() - startTime;
            
            System.out.println("\nResults:");
            System.out.println("Lines processed: " + summary.lineCount);
            System.out.println("Total characters: " + summary.totalChars);
            System.out.println("Words found: " + summary.wordCount);
            System.out.println("Processing time: " + (duration / 1_000_000) + " ms");
            System.out.println("Threads used: " + ForkJoinPool.commonPool().getParallelism());
        }
    }
    
    // Helper methods
    
    private static void processChunk(byte[] buffer, int offset, int length) {
        // Example processing: count specific bytes
        int count = 0;
        for (int i = offset; i < offset + length; i++) {
            if (buffer[i] == '\n') count++;
        }
        // Your processing logic here
    }
    
    private static void processMappedBuffer(MappedByteBuffer buffer) {
        // Example: scan through mapped memory
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            // Process byte
        }
    }
    
    private static LineProcessingResult processLineIntensively(String line) {
        // Simulate CPU-intensive processing
        String[] words = line.split("\\s+");
        int charCount = line.length();
        int wordCount = words.length;
        
        // Some processing simulation
        for (String word : words) {
            word.hashCode(); // Simulate work
        }
        
        return new LineProcessingResult(charCount, wordCount);
    }
    
    private static boolean isTextFile(Path path) throws IOException {
        // Simple heuristic: check first 1KB for text content
        try (InputStream is = Files.newInputStream(path)) {
            byte[] sample = new byte[1024];
            int bytesRead = is.read(sample);
            
            int textBytes = 0;
            for (int i = 0; i < bytesRead; i++) {
                byte b = sample[i];
                if ((b >= 32 && b <= 126) || b == '\t' || b == '\n' || b == '\r') {
                    textBytes++;
                }
            }
            
            return (double) textBytes / bytesRead > 0.7; // 70% text characters
        }
    }
    
    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "";
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }
    
    // Helper classes
    
    static class StreamSummary {
        long count = 0;
        long totalLength = 0;
        long maxLength = 0;
    }
    
    static class ProcessingSummary {
        long lineCount = 0;
        long totalChars = 0;
        long wordCount = 0;
        
        void accumulate(LineProcessingResult result) {
            lineCount++;
            totalChars += result.charCount;
            wordCount += result.wordCount;
        }
        
        ProcessingSummary combine(ProcessingSummary other) {
            this.lineCount += other.lineCount;
            this.totalChars += other.totalChars;
            this.wordCount += other.wordCount;
            return this;
        }
    }
    
    static class LineProcessingResult {
        final int charCount;
        final int wordCount;
        
        LineProcessingResult(int charCount, int wordCount) {
            this.charCount = charCount;
            this.wordCount = wordCount;
        }
    }
}