import com.linkedin.migz.MiGzInputStream;
import com.linkedin.migz.MiGzOutputStream;
import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Complete example demonstrating MiGz pipeline processing.
 * Pattern B: Pipeline processing - Compress data in one step, decompress in another step.
 */
public class PipelineExample {
    
    public static void main(String[] args) {
        System.out.println("=== MiGz Pipeline Example ===");
        
        try {
            // Example 1: Memory-based pipeline
            memoryPipelineExample();
            
            // Example 2: File-based pipeline  
            filePipelineExample();
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Memory-based pipeline example.
     */
    public static void memoryPipelineExample() throws IOException {
        System.out.println("\n=== Memory Pipeline Example ===");
        
        // Original data
        String originalData = "This is test data that will be compressed and decompressed using MiGz.";
        byte[] inputData = originalData.getBytes(StandardCharsets.UTF_8);
        
        System.out.println("Original data: " + originalData);
        System.out.println("Original size: " + inputData.length + " bytes");
        
        // Step 1: Compress in memory
        System.out.println("\nStep 1: Compressing data...");
        long compressionStart = System.currentTimeMillis();
        
        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        try (MiGzOutputStream migzOut = new MiGzOutputStream(compressedOutput)) {
            migzOut.write(inputData);
        }
        
        byte[] compressedData = compressedOutput.toByteArray();
        long compressionTime = System.currentTimeMillis() - compressionStart;
        
        System.out.println("Compressed size: " + compressedData.length + " bytes");
        System.out.println("Compression time: " + compressionTime + " ms");
        
        // Step 2: Decompress in memory
        System.out.println("\nStep 2: Decompressing data...");
        long decompressionStart = System.currentTimeMillis();
        
        ByteArrayInputStream compressedInput = new ByteArrayInputStream(compressedData);
        ByteArrayOutputStream decompressedOutput = new ByteArrayOutputStream();
        
        try (MiGzInputStream migzIn = new MiGzInputStream(compressedInput)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            
            while ((bytesRead = migzIn.read(buffer)) != -1) {
                decompressedOutput.write(buffer, 0, bytesRead);
            }
        }
        
        byte[] decompressedData = decompressedOutput.toByteArray();
        long decompressionTime = System.currentTimeMillis() - decompressionStart;
        
        String decompressedMessage = new String(decompressedData, StandardCharsets.UTF_8);
        System.out.println("Decompressed data: " + decompressedMessage);
        System.out.println("Decompressed size: " + decompressedData.length + " bytes");
        System.out.println("Decompression time: " + decompressionTime + " ms");
        
        // Verify data integrity
        boolean dataMatches = java.util.Arrays.equals(inputData, decompressedData);
        System.out.println("Data integrity check: " + (dataMatches ? "PASSED" : "FAILED"));
        
        System.out.println("\n=== Results ===");
        System.out.println("Compression ratio: " + String.format("%.2f%%", 
            (1.0 - (double) compressedData.length / inputData.length) * 100));
        System.out.println("Total processing time: " + (compressionTime + decompressionTime) + " ms");
    }
    
    /**
     * File-based pipeline example.
     */
    public static void filePipelineExample() throws IOException {
        System.out.println("\n=== File Pipeline Example ===");
        
        // Create test files
        String inputFile = "test_input.txt";
        String compressedFile = "compressed.migz";
        String decompressedFile = "decompressed.txt";
        
        // Create test data file
        try (FileWriter writer = new FileWriter(inputFile)) {
            for (int i = 0; i < 100; i++) {
                writer.write("This is test data line " + i + " with some content to compress.\n");
            }
        }
        
        // Step 1: Compress file
        System.out.println("Step 1: Compressing file...");
        long compressionStart = System.currentTimeMillis();
        
        try (FileInputStream fis = new FileInputStream(inputFile);
             FileOutputStream fos = new FileOutputStream(compressedFile);
             MiGzOutputStream migzOut = new MiGzOutputStream(fos)) {
            
            byte[] buffer = new byte[1024];
            int bytesRead;
            
            while ((bytesRead = fis.read(buffer)) != -1) {
                migzOut.write(buffer, 0, bytesRead);
            }
        }
        
        long compressionTime = System.currentTimeMillis() - compressionStart;
        System.out.println("Compression completed in: " + compressionTime + " ms");
        
        // Step 2: Decompress file
        System.out.println("Step 2: Decompressing file...");
        long decompressionStart = System.currentTimeMillis();
        
        try (FileInputStream fis = new FileInputStream(compressedFile);
             FileOutputStream fos = new FileOutputStream(decompressedFile);
             MiGzInputStream migzIn = new MiGzInputStream(fis)) {
            
            byte[] buffer = new byte[1024];
            int bytesRead;
            
            while ((bytesRead = migzIn.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }
        
        long decompressionTime = System.currentTimeMillis() - decompressionStart;
        System.out.println("Decompression completed in: " + decompressionTime + " ms");
        
        // Show results
        File originalFile = new File(inputFile);
        File compressedFileObj = new File(compressedFile);
        File decompressedFileObj = new File(decompressedFile);
        
        System.out.println("\n=== File Pipeline Results ===");
        System.out.println("Original file size: " + originalFile.length() + " bytes");
        System.out.println("Compressed file size: " + compressedFileObj.length() + " bytes");
        System.out.println("Decompressed file size: " + decompressedFileObj.length() + " bytes");
        System.out.println("Compression ratio: " + String.format("%.2f%%", 
            (1.0 - (double) compressedFileObj.length() / originalFile.length()) * 100));
        System.out.println("Compression time: " + compressionTime + " ms");
        System.out.println("Decompression time: " + decompressionTime + " ms");
        
        // Clean up test files
        new File(inputFile).delete();
        new File(compressedFile).delete();
        new File(decompressedFile).delete();
    }
}

/*
 * USAGE:
 * java PipelineExample
 * 
 * KEY CONCEPTS DEMONSTRATED:
 * 
 * 1. Compression Pipeline:
 *    Original Data → MiGzOutputStream → Compressed Data → Stream/File
 * 
 * 2. Decompression Pipeline:
 *    Stream/File → Compressed Data → MiGzInputStream → Original Data
 * 
 * 3. Pattern B: Pipeline Processing:
 *    - Compress data in one step (sender/creator)
 *    - Decompress data in another step (receiver/consumer)
 * 
 * 4. Benefits:
 *    - Reduced data size for storage/transmission
 *    - Faster data transfer over networks
 *    - Multithreaded compression/decompression
 */ 