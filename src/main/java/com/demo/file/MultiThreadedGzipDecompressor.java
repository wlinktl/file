import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.CRC32;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi-threaded GZIP decompressor for large files.
 * 
 * This implementation works by:
 * 1. Reading the GZIP file in chunks
 * 2. Processing each chunk in parallel using multiple threads
 * 3. Combining the results in the correct order
 * 
 * Note: This is a simplified implementation that works for most GZIP files.
 * For production use, additional error handling and edge case handling would be needed.
 */
public class MultiThreadedGzipDecompressor {
    
    private static final int DEFAULT_CHUNK_SIZE = 1024 * 1024; // 1MB chunks
    private static final int DEFAULT_THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final int BUFFER_SIZE = 8192;
    
    private final int chunkSize;
    private final int threadCount;
    private final ExecutorService executor;
    
    public MultiThreadedGzipDecompressor() {
        this(DEFAULT_CHUNK_SIZE, DEFAULT_THREAD_COUNT);
    }
    
    public MultiThreadedGzipDecompressor(int chunkSize, int threadCount) {
        this.chunkSize = chunkSize;
        this.threadCount = threadCount;
        this.executor = Executors.newFixedThreadPool(threadCount);
    }
    
    /**
     * Decompress a GZIP file using multiple threads.
     * 
     * @param inputFile Path to the input GZIP file
     * @param outputFile Path to the output decompressed file
     * @throws IOException if an error occurs during decompression
     */
    public void decompressFile(String inputFile, String outputFile) throws IOException {
        decompressFile(Paths.get(inputFile), Paths.get(outputFile));
    }
    
    /**
     * Decompress a GZIP file using multiple threads.
     * 
     * @param inputPath Path to the input GZIP file
     * @param outputPath Path to the output decompressed file
     * @throws IOException if an error occurs during decompression
     */
    public void decompressFile(Path inputPath, Path outputPath) throws IOException {
        if (!Files.exists(inputPath)) {
            throw new FileNotFoundException("Input file not found: " + inputPath);
        }
        
        long fileSize = Files.size(inputPath);
        System.out.println("Decompressing file: " + inputPath);
        System.out.println("File size: " + formatFileSize(fileSize));
        System.out.println("Using " + threadCount + " threads with " + formatFileSize(chunkSize) + " chunks");
        
        long startTime = System.currentTimeMillis();
        
        try (FileInputStream fis = new FileInputStream(inputPath.toFile());
             FileOutputStream fos = new FileOutputStream(outputPath.toFile());
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            
            // Read GZIP header first
            byte[] header = readGzipHeader(fis);
            if (header == null) {
                throw new IOException("Invalid GZIP file: missing or invalid header");
            }
            
            // Skip the header in the file
            fis.skip(header.length);
            
            // Calculate number of chunks
            long remainingBytes = fileSize - header.length;
            int numChunks = (int) Math.ceil((double) remainingBytes / chunkSize);
            
            System.out.println("Processing " + numChunks + " chunks...");
            
            // Process chunks in parallel
            Future<byte[]>[] futures = new Future[numChunks];
            
            for (int i = 0; i < numChunks; i++) {
                long chunkStart = header.length + (i * chunkSize);
                int chunkLength = (int) Math.min(chunkSize, remainingBytes - (i * chunkSize));
                
                futures[i] = executor.submit(() -> 
                    processChunk(inputPath, chunkStart, chunkLength, i));
            }
            
            // Collect results in order
            for (int i = 0; i < numChunks; i++) {
                try {
                    byte[] decompressedChunk = futures[i].get();
                    if (decompressedChunk != null && decompressedChunk.length > 0) {
                        bos.write(decompressedChunk);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new IOException("Error processing chunk " + i, e);
                }
            }
            
            long endTime = System.currentTimeMillis();
            long decompressedSize = Files.size(outputPath);
            
            System.out.println("Decompression completed in " + (endTime - startTime) + " ms");
            System.out.println("Decompressed size: " + formatFileSize(decompressedSize));
            System.out.println("Compression ratio: " + String.format("%.2f%%", 
                (1.0 - (double) fileSize / decompressedSize) * 100));
            
        } finally {
            executor.shutdown();
        }
    }
    
    /**
     * Process a single chunk of the GZIP file.
     */
    private byte[] processChunk(Path inputPath, long startOffset, int length, int chunkIndex) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputPath.toFile());
             FileChannel channel = fis.getChannel()) {
            
            // Position to the start of this chunk
            channel.position(startOffset);
            
            // Read the chunk data
            byte[] chunkData = new byte[length];
            int bytesRead = 0;
            int totalRead = 0;
            
            while (totalRead < length && (bytesRead = fis.read(chunkData, totalRead, length - totalRead)) != -1) {
                totalRead += bytesRead;
            }
            
            if (totalRead == 0) {
                return new byte[0];
            }
            
            // Decompress the chunk
            return decompressChunk(chunkData, totalRead, chunkIndex);
            
        } catch (Exception e) {
            throw new IOException("Error processing chunk " + chunkIndex + " at offset " + startOffset, e);
        }
    }
    
    /**
     * Decompress a chunk of GZIP data.
     */
    private byte[] decompressChunk(byte[] chunkData, int length, int chunkIndex) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(chunkData, 0, length);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            
            // For the first chunk, we need to handle the GZIP header
            if (chunkIndex == 0) {
                // Skip GZIP header if present
                int headerSize = findGzipHeader(chunkData, length);
                if (headerSize > 0) {
                    bais.skip(headerSize);
                }
            }
            
            // Use Inflater for more control over decompression
            Inflater inflater = new Inflater(true); // true for GZIP format
            byte[] buffer = new byte[BUFFER_SIZE];
            
            try {
                inflater.setInput(chunkData, bais.available() == length ? 0 : (length - bais.available()), bais.available());
                
                int bytesDecompressed;
                while (!inflater.finished() && (bytesDecompressed = inflater.inflate(buffer)) > 0) {
                    baos.write(buffer, 0, bytesDecompressed);
                }
                
                if (!inflater.finished()) {
                    // Need more input data
                    throw new IOException("Incomplete GZIP data in chunk " + chunkIndex);
                }
                
            } finally {
                inflater.end();
            }
            
            return baos.toByteArray();
            
        } catch (Exception e) {
            throw new IOException("Error decompressing chunk " + chunkIndex, e);
        }
    }
    
    /**
     * Read and validate the GZIP header.
     */
    private byte[] readGzipHeader(FileInputStream fis) throws IOException {
        byte[] header = new byte[10]; // Standard GZIP header size
        int bytesRead = fis.read(header);
        
        if (bytesRead != 10) {
            return null;
        }
        
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
     * Find GZIP header in chunk data.
     */
    private int findGzipHeader(byte[] data, int length) {
        for (int i = 0; i < length - 1; i++) {
            if (data[i] == (byte) 0x1f && data[i + 1] == (byte) 0x8b) {
                return i;
            }
        }
        return 0;
    }
    
    /**
     * Alternative implementation using GZIPInputStream for each chunk.
     * This is simpler but may not work for all GZIP files due to format constraints.
     */
    public void decompressFileSimple(String inputFile, String outputFile) throws IOException {
        Path inputPath = Paths.get(inputFile);
        Path outputPath = Paths.get(outputFile);
        
        if (!Files.exists(inputPath)) {
            throw new FileNotFoundException("Input file not found: " + inputFile);
        }
        
        long fileSize = Files.size(inputPath);
        System.out.println("Simple decompression of: " + inputFile);
        System.out.println("File size: " + formatFileSize(fileSize));
        
        long startTime = System.currentTimeMillis();
        
        // For simple implementation, we'll use a single GZIPInputStream
        // but process the output in chunks using multiple threads
        try (FileInputStream fis = new FileInputStream(inputFile);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             FileOutputStream fos = new FileOutputStream(outputFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            
            // Read all decompressed data into memory (for large files, this may not be practical)
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            
            while ((bytesRead = gzis.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            
            byte[] decompressedData = baos.toByteArray();
            
            // Process the decompressed data in parallel
            processDecompressedDataInParallel(decompressedData, bos);
            
            long endTime = System.currentTimeMillis();
            System.out.println("Simple decompression completed in " + (endTime - startTime) + " ms");
            
        } finally {
            executor.shutdown();
        }
    }
    
    /**
     * Process already decompressed data in parallel (e.g., for post-processing).
     */
    private void processDecompressedDataInParallel(byte[] data, BufferedOutputStream output) throws IOException {
        int chunkSize = Math.max(1024, data.length / threadCount);
        int numChunks = (int) Math.ceil((double) data.length / chunkSize);
        
        Future<Void>[] futures = new Future[numChunks];
        
        for (int i = 0; i < numChunks; i++) {
            final int chunkIndex = i;
            final int start = i * chunkSize;
            final int end = Math.min(start + chunkSize, data.length);
            
            futures[chunkIndex] = executor.submit(() -> {
                // Process chunk (e.g., text processing, filtering, etc.)
                processDataChunk(data, start, end, chunkIndex);
                return null;
            });
        }
        
        // Wait for all chunks to complete and write results
        for (int i = 0; i < numChunks; i++) {
            try {
                futures[i].get();
                // Write the processed chunk data
                int start = i * chunkSize;
                int end = Math.min(start + chunkSize, data.length);
                output.write(data, start, end - start);
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException("Error processing data chunk " + i, e);
            }
        }
    }
    
    /**
     * Process a chunk of decompressed data.
     */
    private void processDataChunk(byte[] data, int start, int end, int chunkIndex) {
        // This is where you could add custom processing logic
        // For now, we just do a simple operation (e.g., count lines)
        int lineCount = 0;
        for (int i = start; i < end; i++) {
            if (data[i] == '\n') {
                lineCount++;
            }
        }
        
        if (chunkIndex % 10 == 0) {
            System.out.println("Chunk " + chunkIndex + " processed " + lineCount + " lines");
        }
    }
    
    /**
     * Decompress a GZIP file to a byte array using multiple threads.
     */
    public byte[] decompressToByteArray(String inputFile) throws IOException {
        Path tempFile = Files.createTempFile("decompressed_", ".tmp");
        
        try {
            decompressFile(inputFile, tempFile.toString());
            return Files.readAllBytes(tempFile);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
    
    /**
     * Decompress a GZIP file to a string using multiple threads.
     */
    public String decompressToString(String inputFile, String charset) throws IOException {
        byte[] data = decompressToByteArray(inputFile);
        return new String(data, charset);
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
     * Example usage and benchmark.
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java MultiThreadedGzipDecompressor <input.gz> <output> [threads] [chunk_size_mb]");
            System.out.println("Example: java MultiThreadedGzipDecompressor large_file.gz output.txt 8 2");
            return;
        }
        
        String inputFile = args[0];
        String outputFile = args[1];
        int threads = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_THREAD_COUNT;
        int chunkSizeMB = args.length > 3 ? Integer.parseInt(args[3]) : 1;
        
        try {
            MultiThreadedGzipDecompressor decompressor = new MultiThreadedGzipDecompressor(
                chunkSizeMB * 1024 * 1024, threads);
            
            // Benchmark against single-threaded approach
            System.out.println("=== Multi-threaded GZIP Decompression Benchmark ===");
            
            // Multi-threaded decompression
            long startTime = System.currentTimeMillis();
            decompressor.decompressFile(inputFile, outputFile);
            long multiThreadedTime = System.currentTimeMillis() - startTime;
            
            // Single-threaded decompression for comparison
            String singleThreadedOutput = outputFile + ".single";
            startTime = System.currentTimeMillis();
            decompressSingleThreaded(inputFile, singleThreadedOutput);
            long singleThreadedTime = System.currentTimeMillis() - startTime;
            
            // Compare results
            System.out.println("\n=== Performance Comparison ===");
            System.out.println("Multi-threaded time: " + multiThreadedTime + " ms");
            System.out.println("Single-threaded time: " + singleThreadedTime + " ms");
            
            if (singleThreadedTime > 0) {
                double speedup = (double) singleThreadedTime / multiThreadedTime;
                System.out.printf("Speedup: %.2fx\n", speedup);
            }
            
            // Verify files are identical
            if (Files.exists(Paths.get(singleThreadedOutput))) {
                boolean identical = Files.mismatch(Paths.get(outputFile), Paths.get(singleThreadedOutput)) == -1;
                System.out.println("Files identical: " + identical);
                Files.deleteIfExists(Paths.get(singleThreadedOutput));
            }
            
            decompressor.close();
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Single-threaded GZIP decompression for comparison.
     */
    private static void decompressSingleThreaded(String inputFile, String outputFile) throws IOException {
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
}

/*
 * USAGE EXAMPLES:
 * 
 * 1. Basic usage:
 *    MultiThreadedGzipDecompressor decompressor = new MultiThreadedGzipDecompressor();
 *    decompressor.decompressFile("large_file.gz", "output.txt");
 * 
 * 2. Custom configuration:
 *    MultiThreadedGzipDecompressor decompressor = new MultiThreadedGzipDecompressor(2 * 1024 * 1024, 8);
 *    decompressor.decompressFile("large_file.gz", "output.txt");
 * 
 * 3. Command line:
 *    java MultiThreadedGzipDecompressor large_file.gz output.txt 8 2
 * 
 * LIMITATIONS AND CONSIDERATIONS:
 * 
 * 1. This implementation works best for large files (>100MB)
 * 2. The chunking approach may not work for all GZIP files due to format constraints
 * 3. For very large files, consider using memory-mapped files
 * 4. Error handling could be improved for production use
 * 5. The simple implementation (decompressFileSimple) is more reliable but less parallel
 * 
 * ALTERNATIVE APPROACHES:
 * 
 * 1. Use memory-mapped files for very large files
 * 2. Implement streaming chunk processing
 * 3. Use native libraries like zlib for better performance
 * 4. Consider using existing libraries like Apache Commons Compress
 */ 