package com.demo.file;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

/**
 * Utility to generate test GZIP files for benchmarking.
 * Creates a seed GZIP file from csv_1.csv, then generates larger test files.
 */
public class TestFileGenerator {
    private static final String RESOURCES_DIR = "src/main/resources/";
    private static final String CSV_FILE = RESOURCES_DIR + "csv_1.csv";
    private static final String SEED_GZIP = RESOURCES_DIR + "seed_1mb.gz";

    public static void main(String[] args) throws IOException {
        // First, create a seed GZIP file of 1MB
        createSeedGzipFile();
        
        // Then generate test files of different sizes
        generateTestFile(1, 1);  // test_1.gz - 1MB
        generateTestFile(2, 5);  // test_2.gz - 5MB  
        generateTestFile(3, 10); // test_3.gz - 10MB
        
        System.out.println("Test files generated: test_1.gz (1MB), test_2.gz (5MB), test_3.gz (10MB)");
    }

    /**
     * Creates a seed GZIP file of 1MB uncompressed size from the CSV data.
     * This seed file can be used as a base for generating larger test files.
     */
    private static void createSeedGzipFile() throws IOException {
        System.out.println("Creating seed GZIP file...");
        
        // Read the CSV file
        byte[] csvData = Files.readAllBytes(Paths.get(CSV_FILE));
        
        // Create a 1MB seed file by compressing repeated CSV content
        try (FileOutputStream fos = new FileOutputStream(SEED_GZIP);
             GZIPOutputStream gzos = new GZIPOutputStream(fos)) {
            
            long targetSize = 1024 * 1024; // 1MB uncompressed
            long written = 0;
            
            while (written < targetSize) {
                int toWrite = (int) Math.min(csvData.length, targetSize - written);
                gzos.write(csvData, 0, toWrite);
                written += toWrite;
            }
        }
        
        System.out.println("Created " + SEED_GZIP + " (" + new File(SEED_GZIP).length() / (1024 * 1024) + " MB compressed)");
    }

    /**
     * Generates a test GZIP file of the specified size by compressing repeated CSV data.
     * @param fileNumber The number to use in the filename (e.g., 1 -> test_1.gz)
     * @param targetSizeMB The target uncompressed size in megabytes
     */
    private static void generateTestFile(int fileNumber, int targetSizeMB) throws IOException {
        String outFile = RESOURCES_DIR + "test_" + fileNumber + ".gz";
        
        // Read the CSV file
        byte[] csvData = Files.readAllBytes(Paths.get(CSV_FILE));
        
        // Create a GZIP file by compressing repeated CSV content
        try (FileOutputStream fos = new FileOutputStream(outFile);
             GZIPOutputStream gzos = new GZIPOutputStream(fos)) {
            
            long targetSize = targetSizeMB * 1024L * 1024L; // Convert MB to bytes
            long written = 0;
            
            while (written < targetSize) {
                int toWrite = (int) Math.min(csvData.length, targetSize - written);
                gzos.write(csvData, 0, toWrite);
                written += toWrite;
            }
        }
        
        System.out.println("Created " + outFile + " (" + new File(outFile).length() / (1024 * 1024) + " MB compressed)");
    }
}