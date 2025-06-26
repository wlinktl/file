import java.io.*;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * Multi-threaded GZIP decompressor that uses a pipeline approach
 * to speed up decompression of large files.
 * 
 * Compatible with Java 8+
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
    public void decompress(String inputFilePath, String outputFilePath) throws IOException, InterruptedException, ExecutionException {
        System.out.printf("Starting decompression of %s -> %s%n", inputFilePath, outputFilePath);
        System.out.printf("Using %d processor threads, buffer size: %d KB%n", 
                         processorThreads, bufferSize / 1024);
        
        long startTime = System.currentTimeMillis();
        AtomicLong totalBytesRead = new AtomicLong(0);
        AtomicLong totalBytesWritten = new AtomicLong(0);
        
        // Create blocking queues for the pipeline
        BlockingQueue<DataChunk> readQueue = new ArrayBlockingQueue<DataChunk>(queueSize);
        BlockingQueue<DataChunk> writeQueue = new ArrayBlockingQueue<DataChunk>(queueSize);
        
        // Create thread pool
        ExecutorService executor = Executors.newFixedThreadPool(processorThreads + 2);
        
        try {
            // Reader thread - decompresses data sequentially
            Future<Void> readerTask = executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    FileInputStream fis = null;
                    BufferedInputStream bis = null;
                    GZIPInputStream gzis = null;
                    
                    try {
                        fis = new FileInputStream(inputFilePath);
                        bis = new BufferedInputStream(fis);
                        gzis = new GZIPInputStream(bis, bufferSize);
                        
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
                        
                    } finally {
                        if (gzis != null) try { gzis.close(); } catch (IOException e) { /* ignore */ }
                        if (bis != null) try { bis.close(); } catch (IOException e) { /* ignore */ }
                        if (fis != null) try { fis.close(); } catch (IOException e) { /* ignore */ }
                    }
                    return null;
                }
            });
            
            // Processor threads - process data chunks in parallel
            Future<?>[] processorTasks = new Future<?>[processorThreads];
            for (int i = 0; i < processorThreads; i++) {
                final int threadId = i;
                processorTasks[i] = executor.submit(new Runnable() {
                    @Override
                    public void run() {
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
                    }
                });
            }
            
            // Writer thread - writes data in correct order
            Future<Void> writerTask = executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    FileOutputStream fos = null;
                    BufferedOutputStream bos = null;
                    
                    try {
                        fos = new FileOutputStream(outputFilePath);
                        bos = new BufferedOutputStream(fos);
                        
                        int expectedSequence = 0;
                        ConcurrentHashMap<Integer, DataChunk> buffer = new ConcurrentHashMap<Integer, DataChunk>();
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
                                bos.write(toWrite.data);
                                totalBytesWritten.addAndGet(toWrite.data.length);
                                expectedSequence++;
                                
                                if (expectedSequence % 1000 == 0) {
                                    System.out.printf("Written %d chunks, %.2f MB%n", 
                                                    expectedSequence, totalBytesWritten.get() / (1024.0 * 1024.0));
                                }
                            }
                        }
                        
                    } finally {
                        if (bos != null) try { bos.close(); } catch (IOException e) { /* ignore */ }
                        if (fos != null) try { fos.close(); } catch (IOException e) { /* ignore */ }
                    }
                    return null;
                }
            });
            
            // Wait for all tasks to complete
            readerTask.get();
            for (Future<?> task : processorTasks) {
                task.get();
            }
            writerTask.get();
            
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                        System.err.println("Thread pool did not terminate");
                    }
                }
            } catch (InterruptedException ie) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
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
        // try { Thread.sleep(1); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
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
        public static void compareWithStandardDecompression(String inputFile, String outputFile1, String outputFile2) 
                throws IOException, InterruptedException, ExecutionException {
            
            System.out.println("=== BENCHMARK: Standard vs Parallel Decompression ===");
            
            // Standard decompression
            System.out.println("\n1. Standard GZIP decompression:");
            long startTime = System.currentTimeMillis();
            standardDecompress(inputFile, outputFile1);
            long standardTime = System.currentTimeMillis() - startTime;
            
            // Parallel decompression
            System.out.println("\n2. Parallel GZIP decompression:");
            startTime = System.currentTimeMillis();
            ParallelGzipDecompressor parallelDecompressor = new ParallelGzipDecompressor();
            parallelDecompressor.decompress(inputFile, outputFile2);
            long parallelTime = System.currentTimeMillis() - startTime;
            
            // Results
            System.out.println("\n=== RESULTS ===");
            System.out.printf("Standard decompression: %.2f seconds%n", standardTime / 1000.0);
            System.out.printf("Parallel decompression: %.2f seconds%n", parallelTime / 1000.0);
            System.out.printf("Speedup: %.2fx%n", (double)standardTime / parallelTime);
            
            // Verify files are identical
            boolean identical = compareFiles(outputFile1, outputFile2);
            System.out.printf("Output files identical: %s%n", identical);
        }
        
        private static void standardDecompress(String inputFile, String outputFile) throws IOException {
            FileInputStream fis = null;
            GZIPInputStream gzis = null;
            FileOutputStream fos = null;
            
            try {
                fis = new FileInputStream(inputFile);
                gzis = new GZIPInputStream(fis);
                fos = new FileOutputStream(outputFile);
                
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = gzis.read(buffer)) != -1) {
                    fos.write(buffer, 0, bytesRead);
                }
                
            } finally {
                if (gzis != null) try { gzis.close(); } catch (IOException e) { /* ignore */ }
                if (fis != null) try { fis.close(); } catch (IOException e) { /* ignore */ }
                if (fos != null) try { fos.close(); } catch (IOException e) { /* ignore */ }
            }
        }
        
        private static boolean compareFiles(String file1, String file2) throws IOException {
            FileInputStream fis1 = null;
            FileInputStream fis2 = null;
            
            try {
                fis1 = new FileInputStream(file1);
                fis2 = new FileInputStream(file2);
                
                byte[] buffer1 = new byte[8192];
                byte[] buffer2 = new byte[8192];
                
                int read1, read2;
                while ((read1 = fis1.read(buffer1)) != -1) {
                    read2 = fis2.read(buffer2);
                    if (read1 != read2) {
                        return false;
                    }
                    for (int i = 0; i < read1; i++) {
                        if (buffer1[i] != buffer2[i]) {
                            return false;
                        }
                    }
                }
                
                return fis2.read() == -1; // Check if second file also ended
                
            } finally {
                if (fis1 != null) try { fis1.close(); } catch (IOException e) { /* ignore */ }
                if (fis2 != null) try { fis2.close(); } catch (IOException e) { /* ignore */ }
            }
        }
    }
    
    /**
     * Main method with usage examples
     */
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        if (args.length < 2) {
            System.out.println("Usage: java ParallelGzipDecompressor <input.gz> <output>");
            System.out.println("   or: java ParallelGzipDecompressor <input.gz> <output1> <output2> (benchmark mode)");
            return;
        }
        
        String inputFile = args[0];
        File input = new File(inputFile);
        if (!input.exists()) {
            System.err.println("Input file does not exist: " + inputFile);
            return;
        }
        
        if (args.length == 3) {
            // Benchmark mode
            String outputFile1 = args[1];
            String outputFile2 = args[2];
            Benchmark.compareWithStandardDecompression(inputFile, outputFile1, outputFile2);
        } else {
            // Regular mode
            String outputFile = args[1];
            ParallelGzipDecompressor decompressor = new ParallelGzipDecompressor();
            decompressor.decompress(inputFile, outputFile);
        }
    }
}