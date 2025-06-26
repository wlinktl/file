package com.demo.file;

import com.linkedin.migz.MiGzInputStream;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

/**
 * Example class demonstrating how to decompress both MiGz and standard GZIP files.
 * MiGz provides multithreaded decompression for better performance on large files.
 * This class automatically detects the format and uses the appropriate decompressor.
 */
public class MiGzDecompressor {

    /**
     * Decompresses a file using MiGz with automatic format detection.
     * Falls back to standard GZIP if MiGz fails.
     * 
     * @param inputGzipFile Path to the input compressed file
     * @param outputFile Path where the decompressed content will be written
     * @throws IOException if an I/O error occurs
     */
    public static void decompressFile(String inputGzipFile, String outputFile) throws IOException {
        try {
            // Try MiGz first
            decompressWithMigz(inputGzipFile, outputFile);
            System.out.println("Successfully decompressed " + inputGzipFile + " to " + outputFile + " using MiGz");
        } catch (Exception e) {
            // If MiGz fails, try standard GZIP
            System.out.println("MiGz decompression failed, trying standard GZIP...");
            decompressWithGzip(inputGzipFile, outputFile);
            System.out.println("Successfully decompressed " + inputGzipFile + " to " + outputFile + " using GZIP");
        }
    }

    /**
     * Decompresses a file using MiGz with custom thread count.
     * Falls back to standard GZIP if MiGz fails.
     * 
     * @param inputGzipFile Path to the input compressed file
     * @param outputFile Path where the decompressed content will be written
     * @param threadCount Number of threads to use for decompression
     * @throws IOException if an I/O error occurs
     */
    public static void decompressFileWithThreads(String inputGzipFile, String outputFile, int threadCount) throws IOException {
        try {
            // Try MiGz first
            decompressWithMigz(inputGzipFile, outputFile, threadCount);
            System.out.println("Successfully decompressed " + inputGzipFile + " to " + outputFile + 
                             " using MiGz with " + threadCount + " threads");
        } catch (Exception e) {
            // If MiGz fails, try standard GZIP
            System.out.println("MiGz decompression failed, trying standard GZIP...");
            decompressWithGzip(inputGzipFile, outputFile);
            System.out.println("Successfully decompressed " + inputGzipFile + " to " + outputFile + " using GZIP");
        }
    }

    /**
     * Decompresses using MiGz format.
     */
    private static void decompressWithMigz(String inputFile, String outputFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputFile);
             MiGzInputStream migzIn = new MiGzInputStream(fis);
             FileOutputStream fos = new FileOutputStream(outputFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            
            while ((bytesRead = migzIn.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }
        }
    }

    /**
     * Decompresses using MiGz format with custom thread count.
     */
    private static void decompressWithMigz(String inputFile, String outputFile, int threadCount) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputFile);
             MiGzInputStream migzIn = new MiGzInputStream(fis, threadCount);
             FileOutputStream fos = new FileOutputStream(outputFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            
            byte[] buffer = new byte[16384];
            int bytesRead;
            
            while ((bytesRead = migzIn.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }
        }
    }

    /**
     * Decompresses using standard GZIP format.
     */
    private static void decompressWithGzip(String inputFile, String outputFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputFile);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             FileOutputStream fos = new FileOutputStream(outputFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            
            while ((bytesRead = gzis.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }
        }
    }

    /**
     * Decompresses compressed data from memory with automatic format detection.
     * 
     * @param compressedData Byte array containing compressed data
     * @return Decompressed data as byte array
     * @throws IOException if an I/O error occurs
     */
    public static byte[] decompressFromMemory(byte[] compressedData) throws IOException {
        try {
            // Try MiGz first
            return decompressFromMemoryWithMigz(compressedData);
        } catch (Exception e) {
            // If MiGz fails, try standard GZIP
            return decompressFromMemoryWithGzip(compressedData);
        }
    }

    /**
     * Decompresses using MiGz from memory.
     */
    private static byte[] decompressFromMemoryWithMigz(byte[] compressedData) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        try (MiGzInputStream migzIn = new MiGzInputStream(bais)) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            
            while ((bytesRead = migzIn.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
        }
        
        return baos.toByteArray();
    }

    /**
     * Decompresses using standard GZIP from memory.
     */
    private static byte[] decompressFromMemoryWithGzip(byte[] compressedData) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        try (GZIPInputStream gzis = new GZIPInputStream(bais)) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            
            while ((bytesRead = gzis.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
        }
        
        return baos.toByteArray();
    }

    /**
     * Decompresses a file directly to byte array with automatic format detection.
     * 
     * @param inputFile Path to the input compressed file
     * @return Decompressed data as byte array
     * @throws IOException if an I/O error occurs
     */
    public static byte[] decompressFileToByteArray(String inputFile) throws IOException {
        try {
            // Try MiGz first
            return decompressFileToByteArrayWithMigz(inputFile);
        } catch (Exception e) {
            // If MiGz fails, try standard GZIP
            return decompressFileToByteArrayWithGzip(inputFile);
        }
    }

    /**
     * Decompresses using MiGz to byte array.
     */
    private static byte[] decompressFileToByteArrayWithMigz(String inputFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputFile);
             MiGzInputStream migzIn = new MiGzInputStream(fis);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            
            while ((bytesRead = migzIn.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            
            return baos.toByteArray();
        }
    }

    /**
     * Decompresses using standard GZIP to byte array.
     */
    private static byte[] decompressFileToByteArrayWithGzip(String inputFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputFile);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            
            while ((bytesRead = gzis.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            
            return baos.toByteArray();
        }
    }

    /**
     * Decompresses a file and returns the content as a string with automatic format detection.
     * 
     * @param inputFile Path to the input compressed file
     * @param charset Character encoding to use (e.g., "UTF-8")
     * @return Decompressed content as string
     * @throws IOException if an I/O error occurs
     */
    public static String decompressToString(String inputFile, String charset) throws IOException {
        try {
            // Try MiGz first
            return decompressToStringWithMigz(inputFile, charset);
        } catch (Exception e) {
            // If MiGz fails, try standard GZIP
            return decompressToStringWithGzip(inputFile, charset);
        }
    }

    /**
     * Decompresses using MiGz to string.
     */
    private static String decompressToStringWithMigz(String inputFile, String charset) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputFile);
             MiGzInputStream migzIn = new MiGzInputStream(fis);
             InputStreamReader isr = new InputStreamReader(migzIn, charset);
             BufferedReader br = new BufferedReader(isr)) {
            
            StringBuilder sb = new StringBuilder();
            String line;
            
            while ((line = br.readLine()) != null) {
                sb.append(line).append(System.lineSeparator());
            }
            
            return sb.toString();
        }
    }

    /**
     * Decompresses using standard GZIP to string.
     */
    private static String decompressToStringWithGzip(String inputFile, String charset) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputFile);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             InputStreamReader isr = new InputStreamReader(gzis, charset);
             BufferedReader br = new BufferedReader(isr)) {
            
            StringBuilder sb = new StringBuilder();
            String line;
            
            while ((line = br.readLine()) != null) {
                sb.append(line).append(System.lineSeparator());
            }
            
            return sb.toString();
        }
    }

    /**
     * Example usage and performance comparison method.
     */
    public static void main(String[] args) {
        /*
        if (args.length < 2) {
            System.out.println("Usage: java MiGzDecompressor <input.gz> <output>");
            System.out.println("Example: java MiGzDecompressor data.txt.gz data_decompressed.txt");
            return;
        }
        */

        String inputFile = "src/main/resources/test_1.gz";
        String outputFile = "src/main/resources/test_1_decompressed.txt";

        try {
            // Check if input file exists
            if (!Files.exists(Paths.get(inputFile))) {
                System.err.println("Input file does not exist: " + inputFile);
                return;
            }

            // Method 1: Basic decompression with automatic format detection
            System.out.println("Decompressing with automatic format detection...");
            long startTime = System.currentTimeMillis();
            decompressFile(inputFile, outputFile);
            long defaultTime = System.currentTimeMillis() - startTime;
            System.out.println("Decompression took: " + defaultTime + " ms");

            // Method 2: Decompression with custom thread count
            System.out.println("\nDecompressing with custom thread count...");
            startTime = System.currentTimeMillis();
            int threadCount = Runtime.getRuntime().availableProcessors();
            decompressFileWithThreads(inputFile, outputFile + "_threaded", threadCount);
            long threadedTime = System.currentTimeMillis() - startTime;
            System.out.println("Threaded decompression took: " + threadedTime + " ms");

            // Method 3: Decompress file directly to byte array in memory
            System.out.println("\nDecompressing file to byte array in memory...");
            startTime = System.currentTimeMillis();
            byte[] decompressedBytes = decompressFileToByteArray(inputFile);
            long memoryTime = System.currentTimeMillis() - startTime;
            System.out.println("Memory decompression took: " + memoryTime + " ms");
            System.out.println("Decompressed size: " + decompressedBytes.length + " bytes");
            
            // Show first few bytes as hex (useful for binary files)
            System.out.println("First 20 bytes (hex): ");
            for (int i = 0; i < Math.min(20, decompressedBytes.length); i++) {
                System.out.printf("%02X ", decompressedBytes[i]);
            }
            System.out.println();

            // Method 4: For text files, demonstrate string decompression
            if (inputFile.toLowerCase().contains(".txt") || inputFile.toLowerCase().contains("text")) {
                System.out.println("\nDecompressing to string (first 500 characters)...");
                String content = decompressToString(inputFile, "UTF-8");
                System.out.println("Content preview:");
                System.out.println(content.substring(0, Math.min(500, content.length())));
                if (content.length() > 500) {
                    System.out.println("... (truncated)");
                }
            }

        } catch (IOException e) {
            System.err.println("Error during decompression: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

/*
 * MAVEN DEPENDENCY:
 * 
 * Add this to your pom.xml:
 * 
 * <dependency>
 *     <groupId>com.linkedin.migz</groupId>
 *     <artifactId>migz</artifactId>
 *     <version>1.0.0</version>
 * </dependency>
 * 
 * GRADLE DEPENDENCY:
 * 
 * Add this to your build.gradle:
 * 
 * implementation 'com.linkedin.migz:migz:1.0.0'
 * 
 * PERFORMANCE NOTES:
 * 
 * 1. MiGz shows the most benefit on larger files (tens of MBs or more)
 * 2. For smaller files, the overhead might make it slower than standard GZipInputStream
 * 3. The default thread count is usually optimal (number of logical cores for decompression)
 * 4. This class automatically detects the format and uses the appropriate decompressor
 * 5. MiGz files are fully compatible with standard gzip utilities
 * 6. Standard GZIP files will be decompressed using GZIPInputStream as fallback
 */