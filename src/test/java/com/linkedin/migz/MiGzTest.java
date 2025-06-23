/*
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.migz;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;


public class MiGzTest {
  private static final int DEFAULT_THREAD_COUNT = Runtime.getRuntime().availableProcessors();

  /**
   * Tests the compression of (almost always) incompressible pseudorandom data.  This is useful for exercising the edge
   * case wherein DEFLATE stores data in uncompressed blocks that are larger than the original data.
   *
   * @throws IOException nominally, but not in practice since in-memory streams are used
   */
  @Test
  public void testRandomDataCompression() throws IOException {
    System.out.println("testRandomDataCompression");
    Random r = new Random(1);
    byte[][] buffers = new byte[][]{new byte[100], new byte[1000], new byte[10000], new byte[100000]};

    for (int i = 0; i < 1000; i++) {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
          MiGzOutputStream mzos = new MiGzOutputStream(baos, DEFAULT_THREAD_COUNT, 50 * 1024)) {
        mzos.setCompressionLevel(3);

        for (int j = 0; j < 10; j++) {
          for (byte[] buffer : buffers) {
            r.nextBytes(buffer);
            mzos.write(buffer);
          }
        }
      }
    }
  }

  @Test
  public void testOutput() throws IOException {
    System.out.println("testOutput");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    MiGzOutputStream mzos = new MiGzOutputStream(baos, 16, 2);
    mzos.write(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
    mzos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    MiGzInputStream mzip = new MiGzInputStream(bais, new ForkJoinPool(20));

    for (int i = 0; i < 16; i++) {
      System.out.println(mzip.read());
    }
  }

  @Test
  public void testShakeswordWithVaryingFlushRates() throws IOException {
    System.out.println("testShakeswordWithVaryingFlushRates");
    testShakesword(0, 100);
    testShakesword(0, 1000);
    testShakesword(0.02, 1000);
    testShakesword(0.05, 1000);
    testShakesword(0.2, 1000);
    testShakesword(0.8, 1000);
  }

  public void testShakesword(double flushRate, int blockSize) throws IOException {
    java.io.InputStream shakestream = MiGzTest.class.getResourceAsStream("/shakespeare.tar");
    
    // Use temporary file for large datasets instead of ByteArrayOutputStream
    java.io.File tempFile = java.io.File.createTempFile("migz_test_", ".gz");
    tempFile.deleteOnExit();
    
    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(tempFile);
         MiGzOutputStream mzos = new MiGzOutputStream(fos, 8, blockSize)) {
      byte[] buffer = new byte[1024 * 16];
      int read;
      Random r = new Random(1337);

      while ((read = shakestream.read(buffer)) > 0) {
        mzos.write(buffer, 0, read);
        if (r.nextDouble() < flushRate) {
          mzos.flush();
        }
      }
    }

    // Use file-based streams for decompression testing
    try (java.io.FileInputStream fis1 = new java.io.FileInputStream(tempFile);
         java.io.FileInputStream fis2 = new java.io.FileInputStream(tempFile);
         GZIPInputStream gzis = new GZIPInputStream(fis1);
         MiGzInputStream mzis = new MiGzInputStream(fis2, new ForkJoinPool(10))) {

      shakestream = MiGzTest.class.getResourceAsStream("/shakespeare.tar");

      int byte1;
      int byte2;
      int byte3;

      int count = 0;
      do {
        byte1 = shakestream.read();
        byte2 = mzis.read();
        byte3 = gzis.read();

        assertEquals("Error reading with MiGzInputStream on byte " + (count++), byte1, byte2);
        assertEquals("Error reading with GZipInputStream on byte " + (count++), byte1, byte3);
      } while (byte1 != -1);
    }

    // Test closing behavior with file-based stream
    try (java.io.FileInputStream fis3 = new java.io.FileInputStream(tempFile);
         MiGzInputStream mzisToClose = new MiGzInputStream(fis3)) {
      for (int i = 0; i < 16; i++) {
        mzisToClose.readBuffer();
      }
      mzisToClose.close(); // make sure this closes successfully, without hanging the test or throwing
    }
    
    assertTrue(ForkJoinPool.commonPool().awaitQuiescence(5, TimeUnit.SECONDS)); // make sure threads are idle
  }

  private static void copyShakestream(OutputStream target) throws IOException {
    try (InputStream shakestream = MiGzTest.class.getResourceAsStream("/shakespeare.tar")) {
      byte[] buffer = new byte[1024 * 16];
      int read;

      while ((read = shakestream.read(buffer)) > 0) {
        target.write(buffer, 0, read);
      }
    }
  }

  @Test
  public void decompressionSpeedTest() throws IOException {
    System.out.println("decompressionSpeedTest");
    
    // Use temporary file instead of ByteArrayOutputStream for large datasets
    java.io.File tempFile = java.io.File.createTempFile("migz_decomp_test_", ".gz");
    tempFile.deleteOnExit();
    
    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(tempFile);
         MiGzOutputStream mzos = new MiGzOutputStream(fos).setCompressionLevel(Deflater.DEFAULT_COMPRESSION)) {
      copyShakestream(mzos);
    }

    long time = System.currentTimeMillis();
    byte[] readBuffer = new byte[512 * 1024];
    for (int i = 0; i < 100; i++) {
      try (java.io.FileInputStream fis = new java.io.FileInputStream(tempFile);
           MiGzInputStream mzis = new MiGzInputStream(fis)) {
        while (mzis.read(readBuffer) > 0) {
          // Process data in chunks
        }
      }
    }
    System.out.println(" ****** time taken: " + (System.currentTimeMillis() - time));
  }

  @Test
  public void compressionExceptionTest() throws IOException {
    System.out.println("compressionExceptionTest");
    OutputStream os = new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        throw new ClassCastException();
      }
    };

    MiGzOutputStream mos = new MiGzOutputStream(os);
    try {
      while (true) {
        mos.write(4);
      }
    } catch (ClassCastException e) {
      return;
    }
  }

  @Test
  public void decompressionExceptionTest() throws IOException {
    System.out.println("decompressionExceptionTest");
    InputStream is = new InputStream() {
      @Override
      public int read() throws IOException {
        throw new ClassCastException();
      }
    };

    MiGzInputStream mis = new MiGzInputStream(is);
    try {
      while (true) {
        mis.readBuffer();
      }
    } catch (ClassCastException e) {
      mis.close();
      return;
    }
  }

  /**
   * Efficiently processes very large files by streaming data in chunks instead of loading everything into memory.
   * This method is designed for files larger than 1GB and uses configurable buffer sizes for optimal performance.
   * 
   * @param inputFile The input file to process
   * @param outputFile The output file for compressed data
   * @param bufferSize The buffer size for processing (default: 1MB for large files)
   * @param threadCount Number of threads for parallel processing
   * @throws IOException if an I/O error occurs
   */
  public static void processLargeFileEfficiently(java.io.File inputFile, java.io.File outputFile, 
                                                int bufferSize, int threadCount) throws IOException {
    if (bufferSize <= 0) {
      bufferSize = 1024 * 1024; // Default 1MB buffer for large files
    }
    
    try (java.io.FileInputStream fis = new java.io.FileInputStream(inputFile);
         java.io.FileOutputStream fos = new java.io.FileOutputStream(outputFile);
         MiGzOutputStream mzos = new MiGzOutputStream(fos, threadCount, bufferSize)) {
      
      byte[] buffer = new byte[bufferSize];
      int bytesRead;
      long totalBytesProcessed = 0;
      
      while ((bytesRead = fis.read(buffer)) > 0) {
        mzos.write(buffer, 0, bytesRead);
        totalBytesProcessed += bytesRead;
        
        // Log progress for very large files
        if (totalBytesProcessed % (100 * 1024 * 1024) == 0) { // Every 100MB
          System.out.println("Processed " + (totalBytesProcessed / (1024 * 1024)) + " MB");
        }
      }
    }
  }

  /**
   * Decompresses a large file efficiently using streaming approach.
   * 
   * @param compressedFile The compressed file to decompress
   * @param outputFile The output file for decompressed data
   * @param bufferSize The buffer size for processing
   * @param threadCount Number of threads for parallel processing
   * @throws IOException if an I/O error occurs
   */
  public static void decompressLargeFileEfficiently(java.io.File compressedFile, java.io.File outputFile,
                                                   int bufferSize, int threadCount) throws IOException {
    if (bufferSize <= 0) {
      bufferSize = 1024 * 1024; // Default 1MB buffer
    }
    
    try (java.io.FileInputStream fis = new java.io.FileInputStream(compressedFile);
         java.io.FileOutputStream fos = new java.io.FileOutputStream(outputFile);
         MiGzInputStream mzis = new MiGzInputStream(fis, new ForkJoinPool(threadCount))) {
      
      byte[] buffer = new byte[bufferSize];
      int bytesRead;
      long totalBytesProcessed = 0;
      
      while ((bytesRead = mzis.read(buffer)) > 0) {
        fos.write(buffer, 0, bytesRead);
        totalBytesProcessed += bytesRead;
        
        // Log progress for very large files
        if (totalBytesProcessed % (100 * 1024 * 1024) == 0) { // Every 100MB
          System.out.println("Decompressed " + (totalBytesProcessed / (1024 * 1024)) + " MB");
        }
      }
    }
  }

  /**
   * Test method to demonstrate efficient processing of large files.
   * This test creates a large file and processes it using the streaming approach.
   */
  @Test
  public void testLargeFileProcessing() throws IOException {
    System.out.println("testLargeFileProcessing");
    
    // Create a large test file (100MB for testing purposes)
    java.io.File largeInputFile = java.io.File.createTempFile("large_test_input_", ".dat");
    java.io.File compressedFile = java.io.File.createTempFile("large_test_compressed_", ".gz");
    java.io.File decompressedFile = java.io.File.createTempFile("large_test_decompressed_", ".dat");
    
    try {
      // Create a large test file with random data
      createLargeTestFile(largeInputFile, 100 * 1024 * 1024); // 100MB
      
      // Test compression
      long startTime = System.currentTimeMillis();
      processLargeFileEfficiently(largeInputFile, compressedFile, 1024 * 1024, DEFAULT_THREAD_COUNT);
      long compressionTime = System.currentTimeMillis() - startTime;
      System.out.println("Compression time: " + compressionTime + "ms");
      
      // Test decompression
      startTime = System.currentTimeMillis();
      decompressLargeFileEfficiently(compressedFile, decompressedFile, 1024 * 1024, DEFAULT_THREAD_COUNT);
      long decompressionTime = System.currentTimeMillis() - startTime;
      System.out.println("Decompression time: " + decompressionTime + "ms");
      
      // Verify file integrity
      assertTrue("Compressed file should be smaller than original", 
                compressedFile.length() < largeInputFile.length());
      assertEquals("Decompressed file should match original size", 
                  largeInputFile.length(), decompressedFile.length());
      
    } finally {
      // Clean up temporary files
      largeInputFile.delete();
      compressedFile.delete();
      decompressedFile.delete();
    }
  }

  /**
   * Creates a large test file with random data for testing purposes.
   * 
   * @param file The file to create
   * @param size The size of the file in bytes
   * @throws IOException if an I/O error occurs
   */
  private void createLargeTestFile(java.io.File file, long size) throws IOException {
    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(file)) {
      byte[] buffer = new byte[1024 * 1024]; // 1MB buffer
      Random random = new Random(42); // Fixed seed for reproducible tests
      long remaining = size;
      
      while (remaining > 0) {
        int toWrite = (int) Math.min(remaining, buffer.length);
        random.nextBytes(buffer);
        fos.write(buffer, 0, toWrite);
        remaining -= toWrite;
      }
    }
  }
}
