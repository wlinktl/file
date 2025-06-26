import java.io.*;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * Multi-threaded GZIP decompressor that uses a pipeline approach
 * to speed up decompression of large files.
 * 
 * Uses Java 21+ features including virtual threads and structured concurrency.
 */
public class ParallelGzipDecompressor {
    
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024; // 64KB
    private static final int DEFAULT_QUEUE_SIZE = 100;
    
    private final int bufferSize;
    private final int queueSize;
    private final int processorThreads;
    
    public ParallelGzipDecompressor() {
        this(DEFAULT_BUFFER_SIZE, DEFAULT_QUEUE_SIZE, Runtime.getRuntime().availableProcessors());
    }
    
    public ParallelGzipDecompressor(int bufferSize, int queueSize, int processorThreads) {
        this.bufferSize = bufferSize;
        this.queueSize = queueSize;
        this.processorThreads = processorThreads;
    }
    
    /**
     * Decompresses a gzip file using multiple threads
     */
    public void decompress(Path inputFile, Path outputFile) throws IOException, InterruptedException {
        System.out.printf("Starting decompression of %s -> %s%n", inputFile, outputFile);
        System.out.printf("Using %d processor threads, buffer size: %d KB%n", 
                         processorThreads, bufferSize / 1024);
        
        long startTime = System.currentTimeMillis();
        AtomicLong totalBytesRead = new AtomicLong(0);
        AtomicLong totalBytesWritten = new AtomicLong(0);
        
        // Create blocking queues for the pipeline
        BlockingQueue<DataChunk> readQueue = new ArrayBlockingQueue<>(queueSize);
        BlockingQueue<DataChunk> writeQueue = new ArrayBlockingQueue<>(queueSize);
        
        // Use virtual threads for better resource utilization (Java 21+)
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            
            // Reader thread - decompresses data sequentially
            Future<Void> readerTask = executor.submit(() -> {
                try (var fis = new BufferedInputStream(Files.newInputStream(inputFile));
                     var gzis = new GZIPInputStream(fis, bufferSize)) {
                    
                    byte[] buffer = new byte[bufferSize];
                    int sequenceNumber = 0;
                    int bytesRead;
                    
                    while ((bytesRead = gzis.read(buffer)) != -1) {
                        totalBytesRead.addAndGet(bytesRead);
                        
                        // Create a copy of the data for the queue
                        byte[] data = new byte[bytesRead];
                        System.arraycopy(buffer, 0, data, 0, bytesRead);
                        
                        DataChunk chunk = new DataChunk(data, sequenceNumber++);
                        readQueue.put(chunk);
                        
                        // Progress reporting
                        if (sequenceNumber % 1000 == 0) {
                            System.out.printf("Read %d chunks, %.2f MB processed%n", 
                                            sequenceNumber, totalBytesRead.get() / (1024.0 * 1024.0));
                        }
                    }
                    
                    // Signal end of reading
                    readQueue.put(DataChunk.END_MARKER);
                    
                } catch (Exception e) {
                    throw new RuntimeException("Error in reader thread", e);
                }
                return null;
            });
            
            // Processor threads - process data chunks in parallel
            CompletableFuture<Void>[] processorTasks = new CompletableFuture[processorThreads];
            for (int i = 0; i < processorThreads; i++) {
                final int threadId = i;
                processorTasks[i] = CompletableFuture.runAsync(() -> {
                    try {
                        while (true) {
                            DataChunk chunk = readQueue.take();
                            if (chunk == DataChunk.END_MARKER) {
                                // Pass the end marker to the next stage
                                writeQueue.put(DataChunk.END_MARKER);
                                break;
                            }
                            
                            // Process the chunk (you can add custom processing here)
                            processChunk(chunk, threadId);
                            
                            // Send to writer
                            writeQueue.put(chunk);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Error in processor thread " + threadId, e);
                    }
                }, executor);
            }
            
            // Writer thread - writes data in correct order
            Future<Void> writerTask = executor.submit(() -> {
                try (var fos = new BufferedOutputStream(Files.newOutputStream(outputFile))) {
                    
                    int expectedSequence = 0;
                    var buffer = new ConcurrentHashMap<Integer, DataChunk>();
                    int endMarkersReceived = 0;
                    
                    while (endMarkersReceived < processorThreads) {
                        DataChunk chunk = writeQueue.take();
                        
                        if (chunk == DataChunk.END_MARKER) {
                            endMarkersReceived++;
                            continue;
                        }
                        
                        buffer.put(chunk.sequenceNumber, chunk);
                        
                        // Write chunks in order
                        while (buffer.containsKey(expectedSequence)) {
                            DataChunk toWrite = buffer.remove(expectedSequence);
                            fos.write(toWrite.data);
                            totalBytesWritten.addAndGet(toWrite.data.length);
                            expectedSequence++;
                            
                            if (expectedSequence % 1000 == 0) {
                                System.out.printf("Written %d chunks, %.2f MB%n", 
                                                expectedSequence, totalBytesWritten.get() / (1024.0 * 1024.0));
                            }
                        }
                    }
                    
                } catch (Exception e) {
                    throw new RuntimeException("Error in writer thread", e);
                }
                return null;
            });
            
            // Wait for all tasks to complete
            readerTask.get();
            CompletableFuture.allOf(processorTasks).get();
            writerTask.get();
        }
        
        long endTime = System.currentTimeMillis();
        double durationSeconds = (endTime - startTime) / 1000.0;
        double throughputMBps = (totalBytesWritten.get() / (1024.0 * 1024.0)) / durationSeconds;
        
        System.out.printf("Decompression completed in %.2f seconds%n", durationSeconds);
        System.out.printf("Total bytes read (compressed): %d%n", totalBytesRead.get());
        System.out.printf("Total bytes written (decompressed): %d%n", totalBytesWritten.get());
        System.out.printf("Compression ratio: %.2f%%n", 
                         (1.0 - (double)totalBytesRead.get() / totalBytesWritten.get()) * 100);
        System.out.printf("Throughput: %.2f MB/s%n", throughputMBps);
    }
    
    /**
     * Process a data chunk. Override this method to add custom processing logic.
     */
    protected void processChunk(DataChunk chunk, int threadId) {
        // Example processing: could be data transformation, validation, etc.
        // For pure decompression, this is just a pass-through
        
        // Simulate some processing work (remove in production)
        // Thread.sleep(1); // Uncomment to simulate processing overhead
    }
    
    /**
     * Data chunk representing a piece of decompressed data
     */
    public static class DataChunk {
        public static final DataChunk END_MARKER = new DataChunk(null, -1);
        
        public final byte[] data;
        public final int sequenceNumber;
        
        public DataChunk(byte[] data, int sequenceNumber) {
            this.data = data;
            this.sequenceNumber = sequenceNumber;
        }
    }
    
    /**
     * Simple benchmark utility
     */
    public static class Benchmark {
        public static void compareWithStandardDecompression(Path inputFile, Path outputFile1, Path outputFile2) 
                throws IOException, InterruptedException {
            
            System.out.println("=== BENCHMARK: Standard vs Parallel Decompression ===");
            
            // Standard decompression
            System.out.println("\n1. Standard GZIP decompression:");
            long startTime = System.currentTimeMillis();
            standardDecompress(inputFile, outputFile1);
            long standardTime = System.currentTimeMillis() - startTime;
            
            // Parallel decompression
            System.out.println("\n2. Parallel GZIP decompression:");
            startTime = System.currentTimeMillis();
            var parallelDecompressor = new ParallelGzipDecompressor();
            parallelDecompressor.decompress(inputFile, outputFile2);
            long parallelTime = System.currentTimeMillis() - startTime;
            
            // Results
            System.out.println("\n=== RESULTS ===");
            System.out.printf("Standard decompression: %.2f seconds%n", standardTime / 1000.0);
            System.out.printf("Parallel decompression: %.2f seconds%n", parallelTime / 1000.0);
            System.out.printf("Speedup: %.2fx%n", (double)standardTime / parallelTime);
            
            // Verify files are identical
            boolean identical = Files.mismatch(outputFile1, outputFile2) == -1;
            System.out.printf("Output files identical: %s%n", identical);
        }
        
        private static void standardDecompress(Path inputFile, Path outputFile) throws IOException {
            try (var fis = Files.newInputStream(inputFile);
                 var gzis = new GZIPInputStream(fis);
                 var fos = Files.newOutputStream(outputFile)) {
                
                gzis.transferTo(fos);
            }
        }
    }
    
    /**
     * Main method with usage examples
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.out.println("Usage: java ParallelGzipDecompressor <input.gz> <output>");
            System.out.println("   or: java ParallelGzipDecompressor <input.gz> <output1> <output2> (benchmark mode)");
            return;
        }
        
        Path inputFile = Paths.get(args[0]);
        if (!Files.exists(inputFile)) {
            System.err.println("Input file does not exist: " + inputFile);
            return;
        }
        
        if (args.length == 3) {
            // Benchmark mode
            Path outputFile1 = Paths.get(args[1]);
            Path outputFile2 = Paths.get(args[2]);
            Benchmark.compareWithStandardDecompression(inputFile, outputFile1, outputFile2);
        } else {
            // Regular mode
            Path outputFile = Paths.get(args[1]);
            var decompressor = new ParallelGzipDecompressor();
            decompressor.decompress(inputFile, outputFile);
        }
    }
}