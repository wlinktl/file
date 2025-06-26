package com.demo.file;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Utility class for file operations compatible with Java 8.
 * Provides alternatives to newer Java APIs that aren't available in Java 8.
 */
public class FileUtils {
    
    /**
     * Check if two files are identical (Java 8 compatible alternative to Files.mismatch).
     * 
     * @param file1 First file path
     * @param file2 Second file path
     * @return true if files are identical, false otherwise
     * @throws IOException if an error occurs during file comparison
     */
    public static boolean filesAreIdentical(Path file1, Path file2) throws IOException {
        // Check if both files exist
        if (!Files.exists(file1) || !Files.exists(file2)) {
            return false;
        }
        
        // Check file sizes first (quick check)
        long size1 = Files.size(file1);
        long size2 = Files.size(file2);
        if (size1 != size2) {
            return false;
        }
        
        // Compare file contents byte by byte
        try (InputStream is1 = Files.newInputStream(file1);
             InputStream is2 = Files.newInputStream(file2)) {
            
            byte[] buffer1 = new byte[8192];
            byte[] buffer2 = new byte[8192];
            
            int bytesRead1, bytesRead2;
            while ((bytesRead1 = is1.read(buffer1)) != -1) {
                bytesRead2 = is2.read(buffer2);
                
                if (bytesRead1 != bytesRead2) {
                    return false;
                }
                
                for (int i = 0; i < bytesRead1; i++) {
                    if (buffer1[i] != buffer2[i]) {
                        return false;
                    }
                }
            }
            
            // Check if second file has more data
            return is2.read() == -1;
        }
    }
    
    /**
     * Check if two files are identical using string paths (Java 8 compatible).
     * 
     * @param file1Path First file path as string
     * @param file2Path Second file path as string
     * @return true if files are identical, false otherwise
     * @throws IOException if an error occurs during file comparison
     */
    public static boolean filesAreIdentical(String file1Path, String file2Path) throws IOException {
        return filesAreIdentical(java.nio.file.Paths.get(file1Path), java.nio.file.Paths.get(file2Path));
    }
    
    /**
     * Format file size for display.
     * 
     * @param bytes File size in bytes
     * @return Formatted file size string
     */
    public static String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
} 