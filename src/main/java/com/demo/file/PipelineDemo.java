import com.linkedin.migz.MiGzInputStream;
import com.linkedin.migz.MiGzOutputStream;
import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * MiGz Pipeline Processing Example
 * Pattern B: Compress data in one step, decompress in another step
 */
public class PipelineDemo {
    
    public static void main(String[] args) {
        System.out.println("=== MiGz Pipeline Demo ===");
        
        try {
            memoryPipelineDemo();
            filePipelineDemo();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Memory-based pipeline demonstration
     */
    public static void memoryPipelineDemo() throws IOException {
        System.out.println("\n--- Memory Pipeline Demo ---");
        
        // Original data
        String data = "Hello, this is test data for MiGz compression and decompression!";
        byte[] original = data.getBytes(StandardCharsets.UTF_8);
        
        System.out.println("Original: " + data);
        System.out.println("Size: " + original.length + " bytes");
        
        // Step 1: Compress
        System.out.println("\nStep 1: Compressing...");
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (MiGzOutputStream migzOut = new MiGzOutputStream(compressed)) {
            migzOut.write(original);
        }
        
        byte[] compressedData = compressed.toByteArray();
        System.out.println("Compressed size: " + compressedData.length + " bytes");
        
        // Step 2: Decompress
        System.out.println("\nStep 2: Decompressing...");
        ByteArrayInputStream input = new ByteArrayInputStream(compressedData);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        
        try (MiGzInputStream migzIn = new MiGzInputStream(input)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = migzIn.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
            }
        }
        
        byte[] decompressed = output.toByteArray();
        String result = new String(decompressed, StandardCharsets.UTF_8);
        
        System.out.println("Decompressed: " + result);
        System.out.println("Size: " + decompressed.length + " bytes");
        System.out.println("Success: " + data.equals(result));
    }
    
    /**
     * File-based pipeline demonstration
     */
    public static void filePipelineDemo() throws IOException {
        System.out.println("\n--- File Pipeline Demo ---");
        
        // Create test file
        String inputFile = "test.txt";
        String compressedFile = "test.migz";
        String outputFile = "test_decompressed.txt";
        
        // Write test data
        try (FileWriter writer = new FileWriter(inputFile)) {
            for (int i = 0; i < 50; i++) {
                writer.write("Test line " + i + ": This is sample data for compression.\n");
            }
        }
        
        // Step 1: Compress file
        System.out.println("Step 1: Compressing file...");
        try (FileInputStream fis = new FileInputStream(inputFile);
             FileOutputStream fos = new FileOutputStream(compressedFile);
             MiGzOutputStream migzOut = new MiGzOutputStream(fos)) {
            
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                migzOut.write(buffer, 0, bytesRead);
            }
        }
        
        // Step 2: Decompress file
        System.out.println("Step 2: Decompressing file...");
        try (FileInputStream fis = new FileInputStream(compressedFile);
             FileOutputStream fos = new FileOutputStream(outputFile);
             MiGzInputStream migzIn = new MiGzInputStream(fis)) {
            
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = migzIn.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }
        
        // Show results
        File original = new File(inputFile);
        File compressed = new File(compressedFile);
        File decompressed = new File(outputFile);
        
        System.out.println("\nResults:");
        System.out.println("Original: " + original.length() + " bytes");
        System.out.println("Compressed: " + compressed.length() + " bytes");
        System.out.println("Decompressed: " + decompressed.length() + " bytes");
        System.out.println("Compression ratio: " + 
            String.format("%.1f%%", (1.0 - (double)compressed.length() / original.length()) * 100));
        
        // Cleanup
        original.delete();
        compressed.delete();
        decompressed.delete();
    }
} 