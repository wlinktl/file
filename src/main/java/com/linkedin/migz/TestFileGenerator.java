package com.linkedin.migz;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Utility to generate large test .gz files by concatenating test_1.gz.
 * test_2.gz = test_1.gz x2, test_3.gz = test_1.gz x3, test_4.gz = test_1.gz x4
 */
public class TestFileGenerator {
    private static final String RESOURCES_DIR = "src/test/resources/";
    private static final String SEED_FILE = RESOURCES_DIR + "test_1.gz";

    public static void main(String[] args) throws IOException {
        generateTestFile(2);
        generateTestFile(3);
        generateTestFile(4);
        System.out.println("Test files generated: test_2.gz, test_3.gz, test_4.gz");
    }

    private static void generateTestFile(int multiplier) throws IOException {
        String outFile = RESOURCES_DIR + "test_" + multiplier + ".gz";
        try (FileOutputStream fos = new FileOutputStream(outFile)) {
            byte[] data = Files.readAllBytes(Paths.get(SEED_FILE));
            for (int i = 0; i < multiplier; i++) {
                fos.write(data);
            }
        }
        System.out.println("Created " + outFile + " (" + new File(outFile).length() / (1024 * 1024) + " MB)");
    }
} 