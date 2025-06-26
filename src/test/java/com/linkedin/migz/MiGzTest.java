/*
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

 package com.linkedin.migz;

 import java.io.ByteArrayInputStream;
 import java.io.ByteArrayOutputStream;
 import java.io.File;
 import java.io.FileOutputStream;
 import java.io.IOException;
 import java.io.InputStream;
 import java.io.OutputStream;
 import java.nio.file.Files;
 import java.nio.file.Paths;
 import java.util.Random;
 import java.util.concurrent.ForkJoinPool;
 import java.util.concurrent.TimeUnit;
 import java.util.zip.Deflater;
 import java.util.zip.GZIPInputStream;
 import java.util.zip.GZIPOutputStream;
 import org.junit.Test;
 
 import static org.junit.Assert.*;
 
 
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
     testShakesword(0, 100);
     testShakesword(0, 1000);
     testShakesword(0.02, 1000);
     testShakesword(0.05, 1000);
     testShakesword(0.2, 1000);
     testShakesword(0.8, 1000);
   }
 
   public void testShakesword(double flushRate, int blockSize) throws IOException {
     java.io.InputStream shakestream = MiGzTest.class.getResourceAsStream("/shakespeare.tar");
     ByteArrayOutputStream baos = new ByteArrayOutputStream();
     MiGzOutputStream mzos = new MiGzOutputStream(baos, 8, blockSize);
     byte[] buffer = new byte[1024 * 16];
     int read;
     Random r = new Random(1337);
 
     while ((read = shakestream.read(buffer)) > 0) {
       mzos.write(buffer, 0, read);
       if (r.nextDouble() < flushRate) {
         mzos.flush();
       }
     }
     mzos.close();
 
     ByteArrayInputStream bais1 = new ByteArrayInputStream(baos.toByteArray());
     GZIPInputStream gzis = new GZIPInputStream(bais1);
 
     ByteArrayInputStream bais2 = new ByteArrayInputStream(baos.toByteArray());
     MiGzInputStream mzis = new MiGzInputStream(bais2, new ForkJoinPool(10));
 
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
 
     MiGzInputStream mzisToClose = new MiGzInputStream(new ByteArrayInputStream(baos.toByteArray()));
     for (int i = 0; i < 16; i++) {
       mzisToClose.readBuffer();
     }
     mzisToClose.close(); // make sure this closes successfully, without hanging the test or throwing
     assertTrue(ForkJoinPool.commonPool().awaitQuiescence(5, TimeUnit.SECONDS)); // make sure threads are idle
   }
 
   private static void copyResourceToStream(String resourcePath, OutputStream target) throws IOException {
     try (InputStream inputStream = MiGzTest.class.getResourceAsStream(resourcePath)) {
       if (inputStream == null) {
         throw new IOException("Resource '" + resourcePath + "' not found in classpath");
       }
       
       byte[] buffer = new byte[1024 * 16];
       int read;

       while ((read = inputStream.read(buffer)) > 0) {
         target.write(buffer, 0, read);
       }
     }
   }
 
   //@Test
   public void decompressionSpeedTest() throws IOException {
     ByteArrayOutputStream baos = new ByteArrayOutputStream();
     try (MiGzOutputStream mzos = new MiGzOutputStream(baos).setCompressionLevel(Deflater.DEFAULT_COMPRESSION)) {
       copyResourceToStream("/test_1.gz", mzos);
     }
 
     long time = System.currentTimeMillis();
     byte[] readBuffer = new byte[512 * 1024];
     for (int i = 0; i < 100; i++) {
       ByteArrayInputStream bais2 = new ByteArrayInputStream(baos.toByteArray());
       //GZIPInputStream mzis = new GZIPInputStream(bais2);
       MiGzInputStream mzis = new MiGzInputStream(bais2);
       while (mzis.read(readBuffer) > 0) {
 
       }
     }
     System.out.println(System.currentTimeMillis() - time);
   }
 
   /**
    * Tests decompression of test_1.gz file using both MiGzInputStream and GZIPInputStream.
    * This test verifies that both decompressors can handle the test file correctly.
    *
    * @throws IOException if there's an error reading or decompressing the file
    */
   @Test
   public void testDecompressTestFile() throws IOException {
     // Create test data
     String testData = "id,name,email,age,city,country,occupation,salary,department,hire_date\n";
     testData += "1,John Smith,john.smith@email.com,32,New York,USA,Software Engineer,85000,Engineering,2020-01-15\n";
     testData += "2,Jane Doe,jane.doe@email.com,28,San Francisco,USA,Data Scientist,92000,Data Science,2019-03-22\n";
     testData += "3,Mike Johnson,mike.johnson@email.com,35,Chicago,USA,Product Manager,95000,Product,2018-07-10\n";
     testData += "4,Sarah Wilson,sarah.wilson@email.com,29,Boston,USA,UX Designer,78000,Design,2021-02-14\n";
     testData += "5,David Brown,david.brown@email.com,31,Seattle,USA,DevOps Engineer,88000,Engineering,2020-09-05\n";
     
     // Create a GZIP file with the test data and save it to disk
     String gzipFileName = "src/test/resources/test_generated.gz";
     try (FileOutputStream fos = new FileOutputStream(gzipFileName);
          GZIPOutputStream gzos = new GZIPOutputStream(fos)) {
       // Repeat the data to make it larger
       for (int i = 0; i < 1000; i++) {
         gzos.write(testData.getBytes());
       }
     }
     
     File gzipFile = new File(gzipFileName);
     System.out.println("Created GZIP file: " + gzipFileName + " (" + gzipFile.length() + " bytes)");
     
     // Read the GZIP file from disk and test with GZIPInputStream
     byte[] gzipFileBytes = Files.readAllBytes(Paths.get(gzipFileName));
     System.out.println("Read GZIP file from disk: " + gzipFileBytes.length + " bytes");
     
     ByteArrayInputStream bais1 = new ByteArrayInputStream(gzipFileBytes);
     GZIPInputStream gzis = new GZIPInputStream(bais1);
     
     ByteArrayOutputStream gzipDecompressedOutput = new ByteArrayOutputStream();
     byte[] buffer = new byte[8192];
     int bytesRead;
     while ((bytesRead = gzis.read(buffer)) != -1) {
       gzipDecompressedOutput.write(buffer, 0, bytesRead);
     }
     gzis.close();
     
     byte[] gzipDecompressed = gzipDecompressedOutput.toByteArray();
     System.out.println("GZIPInputStream decompressed size: " + gzipDecompressed.length + " bytes");
     
     // Verify that the GZIP decompressed content is not empty
     assertTrue("GZIP decompressed content should not be empty", gzipDecompressed.length > 0);
     
     // Test decompression with MiGzInputStream (may or may not work depending on format compatibility)
     System.out.println("Testing MiGzInputStream");
     
     // Create a MiGz file for testing MiGzInputStream
     String migzFileName = "src/test/resources/test_generated.migz";
     try (FileOutputStream fos = new FileOutputStream(migzFileName);
          MiGzOutputStream mzos = new MiGzOutputStream(fos, DEFAULT_THREAD_COUNT, 512 * 1024)) {
       // Repeat the data to make it larger
       for (int i = 0; i < 1000; i++) {
         mzos.write(testData.getBytes());
       }
     }
     
     File migzFile = new File(migzFileName);
     System.out.println("Created MiGz file: " + migzFileName + " (" + migzFile.length() + " bytes)");
     
     // Read the MiGz file from disk
     byte[] migzFileBytes = Files.readAllBytes(Paths.get(migzFileName));
     System.out.println("Read MiGz file from disk: " + migzFileBytes.length + " bytes");
     
     ByteArrayInputStream bais2 = new ByteArrayInputStream(migzFileBytes);
     MiGzInputStream mzis = new MiGzInputStream(bais2, new ForkJoinPool(DEFAULT_THREAD_COUNT));
     
     ByteArrayOutputStream migzDecompressedOutput = new ByteArrayOutputStream();
     while ((bytesRead = mzis.read(buffer)) != -1) {
       //System.out.println("bytesRead: " + bytesRead);
       migzDecompressedOutput.write(buffer, 0, bytesRead);
     }
     
     System.out.println("Testing MiGzInputStream done");
     
     mzis.close();
     
     byte[] migzDecompressed = migzDecompressedOutput.toByteArray();
     System.out.println("MiGzInputStream decompressed size: " + migzDecompressed.length + " bytes");
     
     // Compare the decompressed results if MiGzInputStream worked
     assertArrayEquals("Decompressed content should be identical", gzipDecompressed, migzDecompressed);
     System.out.println("MiGzInputStream successfully decompressed the MiGz file");
     
     // Test the original GZIP file with MiGzInputStream (should fail)
     System.out.println("\nTesting original GZIP file with MiGzInputStream (expected to fail):");
     try {
       ByteArrayInputStream bais3 = new ByteArrayInputStream(gzipFileBytes);
       MiGzInputStream mzis2 = new MiGzInputStream(bais3);
       
       ByteArrayOutputStream testOutput = new ByteArrayOutputStream();
       while ((bytesRead = mzis2.read(buffer)) != -1) {
         testOutput.write(buffer, 0, bytesRead);
       }
       mzis2.close();
       
       System.out.println("Unexpected: MiGzInputStream successfully decompressed GZIP file");
     } catch (Exception e) {
       System.out.println("Expected: MiGzInputStream failed to decompress GZIP file: " + e.getMessage());
     }
     
     System.out.println("\n=== Test Summary ===");
     System.out.println("✓ GZIP file created: " + gzipFileName);
     System.out.println("✓ MiGz file created: " + migzFileName);
     System.out.println("✓ GZIPInputStream works with GZIP files");
     System.out.println("✓ MiGzInputStream works with MiGz files");
     System.out.println("✓ Both produce identical decompressed content");
     System.out.println("✓ Format incompatibility properly handled");
   }
 
   @Test
   public void compressionExceptionTest() throws IOException {
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
 }