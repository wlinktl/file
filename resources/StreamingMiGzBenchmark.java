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

/**
 * Enhanced MiGZ Benchmark with Real Streaming Scenarios
 * Tests decompression performance with actual streaming to output files
 */
public class StreamingMiGzBenchmark {
    
    private static final DecimalFormat df = new DecimalFormat("#.##");
    private static final int WARMUP_ITERATIONS = 3;
    private static final int BENCHMARK_ITERATIONS = 10;
    private static final int BUFFER_SIZE = 64 * 1024; // 64KB
    private static final int LARGE_BUFFER_SIZE = 256 * 1024; // 256KB for large files
    private static final int VERY_LARGE_THRESHOLD = 500 * 1024 * 1024; // 500MB
    
    private static final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private static final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    
    enum StreamingMode {
        MEMORY_ONLY,           // Decompress to memory (no I/O)
        STREAM_TO_FILE,        // Stream decompressed data to file
        STREAM_TO_NULL,        // Stream but discard (simulate processing)
        PARALLEL_PROCESSING    // Parallel processing of chunks
    }
    
    public static void main(String[] args) {
        System.out.println("=== Enhanced MiGZ vs Java GZIP Streaming Benchmark ===\n");
        
        // Parse command line arguments
        StreamingMode mode = StreamingMode.STREAM_TO_NULL;
        if (args.length > 0) {
            try {
                mode = StreamingMode.valueOf(args[0].toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid mode. Available modes: MEMORY_ONLY, STREAM_TO_FILE, STREAM_TO_NULL, PARALLEL_PROCESSING");
                System.out.println("Using default: STREAM_TO_NULL");
            }
        }
        
        System.out.println("Benchmark mode: " + mode);
        
        // Get list of test files
        List<String> testFiles = getTestFiles();
        if (testFiles.isEmpty()) {
            System.err.println("No test files found in resources directory!");
            return;
        }
        
        System.out.println("Found " + testFiles.size() + " test files:");
        testFiles.forEach(file -> System.out.println("  - " + file));
        System.out.println();
        
        // Run benchmarks for each file
        for (String testFile : testFiles) {
            runStreamingBenchmark(testFile, mode);
            System.out.println();
        }
        
        System.out.println("=== Streaming Benchmark Complete ===");
    }
    
    private static void runStreamingBenchmark(String filePath, StreamingMode mode) {
        System.out.println("=== Streaming Benchmark: " + filePath + " ===");
        
        File file = new File(filePath);
        if (!file.exists()) {
            System.err.println("File not found: " + filePath);
            return;
        }
        
        long fileSize = file.length();
        System.out.println("File size: " + formatBytes(fileSize));
        System.out.println("Streaming mode: " + mode);
        
        // Choose optimal streaming approach based on file size
        boolean useNIO = fileSize > 100 * 1024 * 1024; // Use NIO for files >100MB
        boolean useDirectBuffers = fileSize > VERY_LARGE_THRESHOLD; // Use direct buffers for very large files
        
        System.out.println("I/O Strategy: " + (useNIO ? "NIO" : "Traditional") + 
                          (useDirectBuffers ? " with Direct Buffers" : ""));
        
        try {
            // Warmup
            System.out.println("\nPerforming warmup runs...");
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                performStreamingDecompression(filePath, true, mode, useNIO, useDirectBuffers, "java");
                performStreamingDecompression(filePath, true, mode, useNIO, useDirectBuffers, "migz");
            }
            
            // Benchmark Java GZIP
            System.out.println("\nBenchmarking Java GZIP streaming decompression...");
            StreamingBenchmarkResult javaResult = benchmarkStreamingMethod(() -> {
                try {
                    return performStreamingDecompression(filePath, false, mode, useNIO, useDirectBuffers, "java");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            
            // Benchmark MiGZ  
            System.out.println("Benchmarking MiGZ streaming decompression...");
            StreamingBenchmarkResult migzResult = benchmarkStreamingMethod(() -> {
                try {
                    return performStreamingDecompression(filePath, false, mode, useNIO, useDirectBuffers, "migz");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            
            // Print results
            printStreamingResults("Java GZIP", javaResult, fileSize);
            printStreamingResults("MiGZ", migzResult, fileSize);
            printStreamingComparison(javaResult, migzResult);
            
        } catch (Exception e) {
            System.err.println("Benchmark failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static StreamingResult performStreamingDecompression(
            String filePath, boolean warmup, StreamingMode mode, 
            boolean useNIO, boolean useDirectBuffers, String method) throws IOException {
        
        long decompressedSize = 0;
        long ioOperations = 0;
        String outputPath = null;
        
        if (mode == StreamingMode.STREAM_TO_FILE && !warmup) {
            outputPath = "output_" + method + "_" + System.currentTimeMillis() + ".dat";
        }
        
        try {
            if (useNIO) {
                StreamingResult result = performNIOStreaming(filePath, outputPath, mode, useDirectBuffers, method);
                return result;
            } else {
                StreamingResult result = performTraditionalStreaming(filePath, outputPath, mode, method);
                return result;
            }
        } finally {
            // Clean up output file if created
            if (outputPath != null) {
                try {
                    Files.deleteIfExists(Paths.get(outputPath));
                } catch (IOException e) {
                    // Ignore cleanup errors
                }
            }
        }
    }
    
    private static StreamingResult performNIOStreaming(
            String filePath, String outputPath, StreamingMode mode, 
            boolean useDirectBuffers, String method) throws IOException {
        
        long decompressedSize = 0;
        long ioOperations = 0;
        Path inputPath = Paths.get(filePath);
        
        try (ReadableByteChannel inputChannel = Files.newByteChannel(inputPath, StandardOpenOption.READ);
             InputStream decompressStream = method.equals("migz") ?
                 new MiGzInputStream(Channels.newInputStream(inputChannel)) :
                 new GZIPInputStream(Channels.newInputStream(inputChannel), LARGE_BUFFER_SIZE);
             WritableByteChannel outputChannel = outputPath != null ?
                 Files.newByteChannel(Paths.get(outputPath), 
                     StandardOpenOption.CREATE, StandardOpenOption.WRITE) : null) {
            
            ReadableByteChannel decompressChannel = Channels.newChannel(decompressStream);
            
            ByteBuffer buffer = useDirectBuffers ? 
                ByteBuffer.allocateDirect(LARGE_BUFFER_SIZE) :
                ByteBuffer.allocate(LARGE_BUFFER_SIZE);
            
            int bytesRead;
            while ((bytesRead = decompressChannel.read(buffer)) != -1) {
                buffer.flip();
                decompressedSize += buffer.remaining();
                
                switch (mode) {
                    case STREAM_TO_FILE:
                        if (outputChannel != null) {
                            while (buffer.hasRemaining()) {
                                outputChannel.write(buffer);
                            }
                            ioOperations++;
                        }
                        break;
                        
                    case STREAM_TO_NULL:
                        // Simulate processing by reading through buffer
                        processBufferContent(buffer);
                        break;
                        
                    case PARALLEL_PROCESSING:
                        // Simulate parallel processing
                        processBufferParallel(buffer);
                        break;
                        
                    case MEMORY_ONLY:
                    default:
                        // Just count bytes
                        break;
                }
                
                buffer.clear();
                ioOperations++;
            }
        }
        
        return new StreamingResult(decompressedSize, ioOperations);
    }
    
    private static StreamingResult performTraditionalStreaming(
            String filePath, String outputPath, StreamingMode mode, String method) throws IOException {
        
        long decompressedSize = 0;
        long ioOperations = 0;
        
        try (FileInputStream fis = new FileInputStream(filePath);
             BufferedInputStream bis = new BufferedInputStream(fis, BUFFER_SIZE * 4);
             InputStream decompressStream = method.equals("migz") ?
                 new MiGzInputStream(bis) :
                 new GZIPInputStream(bis, BUFFER_SIZE);
             OutputStream outputStream = outputPath != null ?
                 new BufferedOutputStream(new FileOutputStream(outputPath), BUFFER_SIZE * 4) : null) {
            
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            
            while ((bytesRead = decompressStream.read(buffer)) != -1) {
                decompressedSize += bytesRead;
                
                switch (mode) {
                    case STREAM_TO_FILE:
                        if (outputStream != null) {
                            outputStream.write(buffer, 0, bytesRead);
                            ioOperations++;
                        }
                        break;
                        
                    case STREAM_TO_NULL:
                        // Simulate processing
                        processArrayContent(buffer, 0, bytesRead);
                        break;
                        
                    case PARALLEL_PROCESSING:
                        // Simulate parallel processing  
                        processArrayParallel(buffer, 0, bytesRead);
                        break;
                        
                    case MEMORY_ONLY:
                    default:
                        // Just count bytes
                        break;
                }
                
                ioOperations++;
            }
        }
        
        return new StreamingResult(decompressedSize, ioOperations);
    }
    
    // Processing simulation methods
    private static void processBufferContent(ByteBuffer buffer) {
        // Simulate real processing: counting, pattern matching, etc.
        int lineCount = 0;
        long checksum = 0;
        
        int startPos = buffer.position();
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            if (b == '\n') lineCount++;
            checksum += b & 0xFF;
        }
        buffer.position(startPos); // Reset for potential output writing
    }
    
    private static void processArrayContent(byte[] buffer, int offset, int length) {
        // Simulate real processing
        int lineCount = 0;
        long checksum = 0;
        
        for (int i = offset; i < offset + length; i++) {
            if (buffer[i] == '\n') lineCount++;
            checksum += buffer[i] & 0xFF;
        }
    }
    
    private static void processBufferParallel(ByteBuffer buffer) {
        // Simulate parallel processing (simplified)
        processBufferContent(buffer);
        // In real scenarios, you might split buffer and process chunks in parallel
    }
    
    private static void processArrayParallel(byte[] buffer, int offset, int length) {
        // Simulate parallel processing
        processArrayContent(buffer, offset, length);
    }
    
    private static StreamingBenchmarkResult benchmarkStreamingMethod(StreamingTask task) {
        List<Long> times = new ArrayList<>();
        List<Long> memoryUsages = new ArrayList<>();
        List<Long> ioOperations = new ArrayList<>();
        long totalDecompressedSize = 0;
        
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            System.gc();
            try { Thread.sleep(100); } catch (InterruptedException e) {}
            
            MemoryUsage memBefore = memoryBean.getHeapMemoryUsage();
            long startTime = System.nanoTime();
            
            StreamingResult result = task.run();
            
            long endTime = System.nanoTime();
            MemoryUsage memAfter = memoryBean.getHeapMemoryUsage();
            
            times.add(endTime - startTime);
            memoryUsages.add(Math.max(0, memAfter.getUsed() - memBefore.getUsed()));
            ioOperations.add(result.ioOperations);
            totalDecompressedSize = result.decompressedSize;
            
            System.out.print(".");
        }
        System.out.println(" Done");
        
        return new StreamingBenchmarkResult(times, memoryUsages, ioOperations, totalDecompressedSize);
    }
    
    private static void printStreamingResults(String method, StreamingBenchmarkResult result, long originalFileSize) {
        System.out.println("\n--- " + method + " Streaming Results ---");
        System.out.println("Decompressed size: " + formatBytes(result.decompressedSize));
        System.out.println("Compression ratio: " + df.format((double) result.decompressedSize / originalFileSize) + ":1");
        
        System.out.println("\nTiming (ms):");
        System.out.println("  Average: " + df.format(result.avgTime / 1_000_000.0));
        System.out.println("  Min: " + df.format(result.minTime / 1_000_000.0));
        System.out.println("  Max: " + df.format(result.maxTime / 1_000_000.0));
        
        System.out.println("\nThroughput:");
        double avgThroughputMBps = (result.decompressedSize / (1024.0 * 1024.0)) / (result.avgTime / 1_000_000_000.0);
        System.out.println("  Decompression: " + df.format(avgThroughputMBps) + " MB/s");
        
        System.out.println("\nI/O Operations:");
        System.out.println("  Average per run: " + result.avgIoOps);
        System.out.println("  I/O