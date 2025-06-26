import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * Multi-threaded GZIP decompressor for large files
 * Uses pipeline processing and concurrent I/O for improved performance
 */
public class MultiThreadGzipDecompressor {
    
    private final int numThreads;
    private final int chunkSize;
    private final int queueCapacity;
    
    /**
     * Progress callback interface
     */
    public interface ProgressCallback {
        void onProgress(double progress, long processed, long total);
    }
    
    /**
     * Data chunk for pipeline processing
     */
    private static class DataChunk {
        private final byte[] data;
        private final int length;
        private final boolean isLast;
        
        public DataChunk(byte[] data, int length, boolean isLast) {
            this.data = data;
            this.length = length;
            this.isLast = isLast;
        }
        
        public byte[] getData() { return data; }
        public int getLength() { return length; }
        public boolean isLast() { return isLast; }
    }
    
    public MultiThreadGzipDecompressor() {
        this(Runtime.getRuntime().availableProcessors(), 1024 * 1024, 10);
    }
    
    public MultiThreadGzipDecompressor(int numThreads, int chunkSize, int queueCapacity) {
        this.numThreads = numThreads;
        this.chunkSize = chunkSize;
        this.queueCapacity = queueCapacity;
    }
    
    /**
     * Decompress a GZIP file using pipeline processing
     */
    public void decompressFile(String inputPath, String outputPath, ProgressCallback callback) 
            throws IOException, InterruptedException {
        
        File inputFile = new File(inputPath);
        long fileSize = inputFile.length();
        AtomicLong processedBytes = new AtomicLong(0);
        
        // Bounded queues for pipeline processing
        BlockingQueue<DataChunk> readQueue = new ArrayBlockingQueue<>(queueCapacity);
        BlockingQueue<DataChunk> writeQueue = new ArrayBlockingQueue<>(queueCapacity);
        
        // Completion flags
        CountDownLatch readComplete = new CountDownLatch(1);
        CountDownLatch decompressComplete = new CountDownLatch(1);
        CountDownLatch writeComplete = new CountDownLatch(1);
        
        // Exception handling
        List<Exception> exceptions = new ArrayList<>();
        
        // Reader thread
        Thread readerThread = new Thread(() -> {
            try (FileInputStream fis = new FileInputStream(inputPath);
                 BufferedInputStream bis = new BufferedInputStream(fis, chunkSize)) {
                
                byte[] buffer = new byte[chunkSize];
                int bytesRead;
                
                while ((bytesRead = bis.read(buffer)) != -1) {
                    byte[] chunk = new byte[bytesRead];
                    System.arraycopy(buffer, 0, chunk, 0, bytesRead);
                    readQueue.put(new DataChunk(chunk, bytesRead, false));
                }
                
                // Signal end of reading
                readQueue.put(new DataChunk(new byte[0], 0, true));
                
            } catch (Exception e) {
                synchronized (exceptions) {
                    exceptions.add(e);
                }
            } finally {
                readComplete.countDown();
            }
        }, "Reader-Thread");
        
        // Decompressor thread
        Thread decompressorThread = new Thread(() -> {
            try {
                PipedOutputStream pos = new PipedOutputStream();
                PipedInputStream pis = new PipedInputStream(pos, chunkSize * 2);
                
                // Start a separate thread to feed data to GZIPInputStream
                CompletableFuture<Void> feederFuture = CompletableFuture.runAsync(() -> {
                    try {
                        while (true) {
                            DataChunk chunk = readQueue.take();
                            if (chunk.isLast()) {
                                pos.close();
                                break;
                            }
                            pos.write(chunk.getData(), 0, chunk.getLength());
                        }
                    } catch (Exception e) {
                        synchronized (exceptions) {
                            exceptions.add(e);
                        }
                    }
                });
                
                // Decompress data
                try (GZIPInputStream gzis = new GZIPInputStream(pis, chunkSize)) {
                    byte[] buffer = new byte[chunkSize];
                    int bytesRead;
                    
                    while ((bytesRead = gzis.read(buffer)) != -1) {
                        byte[] decompressedChunk = new byte[bytesRead];
                        System.arraycopy(buffer, 0, decompressedChunk, 0, bytesRead);
                        writeQueue.put(new DataChunk(decompressedChunk, bytesRead, false));
                    }
                }
                
                feederFuture.get(); // Wait for feeder to complete
                writeQueue.put(new DataChunk(new byte[0], 0, true)); // Signal end
                
            } catch (Exception e) {
                synchronized (exceptions) {
                    exceptions.add(e);
                }
            } finally {
                decompressComplete.countDown();
            }
        }, "Decompressor-Thread");
        
        // Writer thread
        Thread writerThread = new Thread(() -> {
            try (FileOutputStream fos = new FileOutputStream(outputPath);
                 BufferedOutputStream bos = new BufferedOutputStream(fos, chunkSize)) {
                
                while (true) {
                    DataChunk chunk = writeQueue.take();
                    if (chunk.isLast()) {
                        break;
                    }
                    
                    bos.write(chunk.getData(), 0, chunk.getLength());
                    long processed = processedBytes.addAndGet(chunk.getLength());
                    
                    if (callback != null) {
                        double progress = Math.min(100.0, (double) processed / fileSize * 100);
                        callback.onProgress(progress, processed, fileSize);
                    }
                }
                
            } catch (Exception e) {
                synchronized (exceptions) {
                    exceptions.add(e);
                }
            } finally {
                writeComplete.countDown();
            }
        }, "Writer-Thread");
        
        // Start all threads
        readerThread.start();
        decompressorThread.start();
        writerThread.start();
        
        // Wait for completion
        writeComplete.await();
        
        // Check for exceptions
        synchronized (exceptions) {
            if (!exceptions.isEmpty()) {
                throw new IOException("Decompression failed", exceptions.get(0));
            }
        }
    }
    
    /**
     * Optimized streaming decompression using NIO
     */
    public void decompressFileNIO(String inputPath, String outputPath, ProgressCallback callback) 
            throws IOException {
        
        Path input = Paths.get(inputPath);
        Path output = Paths.get(outputPath);
        long fileSize = input.toFile().length();
        AtomicLong processedBytes = new AtomicLong(0);
        
        try (FileChannel inputChannel = FileChannel.open(input, StandardOpenOption.READ);
             FileChannel outputChannel = FileChannel.open(output, StandardOpenOption.CREATE, 
                     StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
             FileInputStream fis = new FileInputStream(inputPath);
             GZIPInputStream gzis = new GZIPInputStream(new BufferedInputStream(fis, chunkSize))) {
            
            ByteBuffer buffer = ByteBuffer.allocateDirect(chunkSize);
            byte[] tempBuffer = new byte[chunkSize];
            
            ExecutorService executor = Executors.newFixedThreadPool(2);
            
            try {
                CompletableFuture<Void> readerFuture = CompletableFuture.runAsync(() -> {
                    try {
                        int bytesRead;
                        while ((bytesRead = gzis.read(tempBuffer)) != -1) {
                            synchronized (buffer) {
                                buffer.clear();
                                buffer.put(tempBuffer, 0, bytesRead);
                                buffer.flip();
                                
                                while (buffer.hasRemaining()) {
                                    outputChannel.write(buffer);
                                }
                                
                                long processed = processedBytes.addAndGet(bytesRead);
                                if (callback != null) {
                                    double progress = Math.min(100.0, (double) processed / fileSize * 100);
                                    callback.onProgress(progress, processed, fileSize);
                                }
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);
                
                readerFuture.get();
                
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException("NIO decompression failed", e);
            } finally {
                executor.shutdown();
            }
        }
    }
}

/**
 * Parallel processor for multiple GZIP files
 */
class ParallelGzipProcessor {
    
    private final int numThreads;
    
    public static class FilePair {
        private final String inputPath;
        private final String outputPath;
        
        public FilePair(String inputPath, String outputPath) {
            this.inputPath = inputPath;
            this.outputPath = outputPath;
        }
        
        public String getInputPath() { return inputPath; }
        public String getOutputPath() { return outputPath; }
    }
    
    public ParallelGzipProcessor() {
        this(Runtime.getRuntime().availableProcessors());
    }
    
    public ParallelGzipProcessor(int numThreads) {
        this.numThreads = numThreads;
    }
    
    /**
     * Decompress multiple files in parallel
     */
    public void decompressMultipleFiles(List<FilePair> filePairs, 
                                      MultiThreadGzipDecompressor.ProgressCallback callback) 
            throws InterruptedException {
        
        AtomicLong completedFiles = new AtomicLong(0);
        long totalFiles = filePairs.size();
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(filePairs.size());
        List<Exception> exceptions = new ArrayList<>();
        
        for (FilePair pair : filePairs) {
            executor.submit(() -> {
                try {
                    decompressSingleFile(pair.getInputPath(), pair.getOutputPath());
                    
                    long completed = completedFiles.incrementAndGet();
                    if (callback != null) {
                        double progress = (double) completed / totalFiles * 100;
                        callback.onProgress(progress, completed, totalFiles);
                    }
                    
                } catch (Exception e) {
                    synchronized (exceptions) {
                        exceptions.add(e);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        executor.shutdown();
        
        if (!exceptions.isEmpty()) {
            System.err.println("Some files failed to decompress:");
            exceptions.forEach(e -> e.printStackTrace());
        }
    }
    
    private void decompressSingleFile(String inputPath, String outputPath) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputPath);
             GZIPInputStream gzis = new GZIPInputStream(new BufferedInputStream(fis));
             FileOutputStream fos = new FileOutputStream(outputPath);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            
            while ((bytesRead = gzis.read(buffer)) != -1) {
                bos.write(buffer