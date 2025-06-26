package com.demo.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Example demonstrating how to use the multi-threaded GZIP decompressors.
 * This shows different approaches for decompressing large GZIP files efficiently.
 */
public class GzipDecompressionExample {
    
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java GzipDecompressionExample <input.gz> [output_dir]");
            System.out.println("Example: java GzipDecompressionExample large_file.gz ./output");
            return;
        }
        
        String inputFile = args[0];
        String outputDir = args.length > 1 ? args[1] : "./decompressed_output";
        
        try {
            // Create output directory
            Path outputPath = Paths.get(outputDir);
            if (!Files.exists(outputPath)) {
                Files.createDirectories(outputPath);
            }
            
            // Run examples
            runExamples(inputFile, outputDir);
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Run various decompression examples.
     */
    public static void runExamples(String inputFile, String outputDir) throws IOException {
        Path inputPath = Paths.get(inputFile);
        
        if (!Files.exists(inputPath)) {
            throw new IOException("Input file not found: " + inputFile);
        }
        
        long fileSize = Files.size(inputPath);
        System.out.println("=== Multi-threaded GZIP Decompression Examples ===");
        System.out.println("Input file: " + inputFile);
        System.out.println("File size: " + formatFileSize(fileSize));
        System.out.println("Output directory: " + outputDir);
        System.out.println();
        
        // Example 1: FastGzipDecompressor (recommended for most cases)
        System.out.println("Example 1: FastGzipDecompressor (Optimized Buffered I/O)");
        exampleFastDecompressor(inputFile, outputDir + "/fast_output.txt");
        
        // Example 2: AdvancedGzipDecompressor (for very large files)
        System.out.println("\nExample 2: AdvancedGzipDecompressor (Memory-mapped + Streaming)");
        exampleAdvancedDecompressor(inputFile, outputDir + "/advanced_output.txt");
        
        // Example 3: Parallel post-processing (for text files that need processing)
        System.out.println("\nExample 3: Parallel Post-processing");
        exampleParallelProcessing(inputFile, outputDir + "/parallel_output.txt");
        
        // Example 4: Decompress to string (for text files)
        System.out.println("\nExample 4: Decompress to String");
        exampleDecompressToString(inputFile);
        
        // Example 5: Custom thread configuration
        System.out.println("\nExample 5: Custom Thread Configuration");
        exampleCustomThreads(inputFile, outputDir + "/custom_output.txt");
        
        System.out.println("\n=== All examples completed successfully! ===");
        System.out.println("Check the output directory for decompressed files.");
    }
    
    /**
     * Example using FastGzipDecompressor.
     */
    private static void exampleFastDecompressor(String inputFile, String outputFile) throws IOException {
        System.out.println("  Using FastGzipDecompressor with default settings...");
        
        long startTime = System.currentTimeMillis();
        
        try (FastGzipDecompressor decompressor = new FastGzipDecompressor()) {
            decompressor.decompressFile(inputFile, outputFile);
        }
        
        long endTime = System.currentTimeMillis();
        long decompressedSize = Files.size(Paths.get(outputFile));
        
        System.out.println("  ✅ Completed in " + (endTime - startTime) + " ms");
        System.out.println("  📁 Output: " + outputFile + " (" + formatFileSize(decompressedSize) + ")");
    }
    
    /**
     * Example using AdvancedGzipDecompressor.
     */
    private static void exampleAdvancedDecompressor(String inputFile, String outputFile) throws IOException {
        System.out.println("  Using AdvancedGzipDecompressor with memory mapping...");
        
        long startTime = System.currentTimeMillis();
        
        try (AdvancedGzipDecompressor decompressor = new AdvancedGzipDecompressor()) {
            decompressor.decompressFile(inputFile, outputFile);
        }
        
        long endTime = System.currentTimeMillis();
        long decompressedSize = Files.size(Paths.get(outputFile));
        
        System.out.println("  ✅ Completed in " + (endTime - startTime) + " ms");
        System.out.println("  📁 Output: " + outputFile + " (" + formatFileSize(decompressedSize) + ")");
    }
    
    /**
     * Example using parallel post-processing.
     */
    private static void exampleParallelProcessing(String inputFile, String outputFile) throws IOException {
        System.out.println("  Using parallel post-processing for text files...");
        
        long startTime = System.currentTimeMillis();
        
        try (AdvancedGzipDecompressor decompressor = new AdvancedGzipDecompressor()) {
            decompressor.decompressWithParallelProcessing(inputFile, outputFile);
        }
        
        long endTime = System.currentTimeMillis();
        long decompressedSize = Files.size(Paths.get(outputFile));
        
        System.out.println("  ✅ Completed in " + (endTime - startTime) + " ms");
        System.out.println("  📁 Output: " + outputFile + " (" + formatFileSize(decompressedSize) + ")");
    }
    
    /**
     * Example decompressing to string.
     */
    private static void exampleDecompressToString(String inputFile) throws IOException {
        System.out.println("  Decompressing to string (for text files)...");
        
        long startTime = System.currentTimeMillis();
        
        try (FastGzipDecompressor decompressor = new FastGzipDecompressor()) {
            String content = decompressor.decompressToString(inputFile, "UTF-8");
            
            long endTime = System.currentTimeMillis();
            
            System.out.println("  ✅ Completed in " + (endTime - startTime) + " ms");
            System.out.println("  📄 Content length: " + content.length() + " characters");
            System.out.println("  📄 First 100 chars: " + content.substring(0, Math.min(100, content.length())) + "...");
        }
    }
    
    /**
     * Example with custom thread configuration.
     */
    private static void exampleCustomThreads(String inputFile, String outputFile) throws IOException {
        System.out.println("  Using custom thread configuration (8 threads)...");
        
        long startTime = System.currentTimeMillis();
        
        try (FastGzipDecompressor decompressor = new FastGzipDecompressor(8)) {
            decompressor.decompressFile(inputFile, outputFile);
        }
        
        long endTime = System.currentTimeMillis();
        long decompressedSize = Files.size(Paths.get(outputFile));
        
        System.out.println("  ✅ Completed in " + (endTime - startTime) + " ms");
        System.out.println("  📁 Output: " + outputFile + " (" + formatFileSize(decompressedSize) + ")");
    }
    
    /**
     * Performance comparison example.
     */
    public static void comparePerformance(String inputFile, String outputDir) throws IOException {
        System.out.println("\n=== Performance Comparison ===");
        
        // Standard decompression
        long startTime = System.currentTimeMillis();
        try (FastGzipDecompressor decompressor = new FastGzipDecompressor()) {
            decompressor.decompressFile(inputFile, outputDir + "/comparison_fast.txt");
        }
        long fastTime = System.currentTimeMillis() - startTime;
        
        // Advanced decompression
        startTime = System.currentTimeMillis();
        try (AdvancedGzipDecompressor decompressor = new AdvancedGzipDecompressor()) {
            decompressor.decompressFile(inputFile, outputDir + "/comparison_advanced.txt");
        }
        long advancedTime = System.currentTimeMillis() - startTime;
        
        // Results
        System.out.println("FastGzipDecompressor: " + fastTime + " ms");
        System.out.println("AdvancedGzipDecompressor: " + advancedTime + " ms");
        
        if (fastTime < advancedTime) {
            System.out.println("🏆 FastGzipDecompressor was faster by " + (advancedTime - fastTime) + " ms");
        } else {
            System.out.println("🏆 AdvancedGzipDecompressor was faster by " + (fastTime - advancedTime) + " ms");
        }
    }
    
    /**
     * Format file size for display.
     */
    private static String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
}

/*
 * USAGE EXAMPLES:
 * 
 * 1. Basic usage:
 *    java GzipDecompressionExample large_file.gz
 * 
 * 2. With custom output directory:
 *    java GzipDecompressionExample large_file.gz ./my_output
 * 
 * 3. Programmatic usage:
 *    // For most files (recommended)
 *    try (FastGzipDecompressor decompressor = new FastGzipDecompressor()) {
 *        decompressor.decompressFile("input.gz", "output.txt");
 *    }
 * 
 *    // For very large files
 *    try (AdvancedGzipDecompressor decompressor = new AdvancedGzipDecompressor()) {
 *        decompressor.decompressFile("large_input.gz", "output.txt");
 *    }
 * 
 *    // For text files that need post-processing
 *    try (AdvancedGzipDecompressor decompressor = new AdvancedGzipDecompressor()) {
 *        decompressor.decompressWithParallelProcessing("input.gz", "output.txt");
 *    }
 * 
 * PERFORMANCE TIPS:
 * 
 * 1. Use FastGzipDecompressor for files < 2GB
 * 2. Use AdvancedGzipDecompressor for files > 2GB
 * 3. Use parallel processing for text files that need filtering/transformation
 * 4. Adjust thread count based on your system's CPU cores
 * 5. Use SSD storage for better I/O performance
 * 
 * WHEN TO USE EACH APPROACH:
 * 
 * - FastGzipDecompressor: Most common use case, good balance of performance and memory usage
 * - AdvancedGzipDecompressor: Very large files, when memory mapping is beneficial
 * - Parallel processing: Text files that need post-processing (filtering, transformation, etc.)
 * - Custom thread count: When you know your system's optimal thread count
 */ 