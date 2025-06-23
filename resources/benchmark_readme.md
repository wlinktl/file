# MiGZ vs Java GZIP Decompression Benchmark

This project provides a comprehensive benchmark comparing LinkedIn's MiGZ multithreaded decompression with Java's built-in single-threaded GZIP decompression.

## Overview

MiGZ is a Java library developed by LinkedIn that provides multithreaded GZIP-compatible compression and decompression. It addresses the bottleneck of single-threaded processing for large files by:
- Using multiple threads for parallel decompression
- Maintaining GZIP compatibility
- Optimizing for large file processing scenarios

## What This Benchmark Measures

### Performance Metrics
- **Decompression Time**: Wall-clock time for complete decompression
- **Throughput**: MB/s decompressed data rate  
- **CPU Time**: Actual CPU time consumed
- **Memory Usage**: Peak and average memory consumption
- **CPU Efficiency**: Ratio of CPU time to wall-clock time

### Test Scenarios
- Multiple file sizes (10MB to 500MB uncompressed)
- Multiple iterations for statistical accuracy
- Warmup runs to eliminate JIT compilation effects
- Resource monitoring throughout execution

## Project Structure

```
├── MiGzBenchmark.java     # Main benchmark implementation
├── pom.xml               # Maven dependencies and build config
├── setup_and_run.sh     # Script to create test files and run benchmark
├── resources/            # Directory containing test .gz files
│   ├── test_1.gz        # Small file (10MB uncompressed)
│   ├── test_2.gz        # Medium file (50MB uncompressed)
│   ├── test_3.gz        # Large file (100MB uncompressed)
│   └── test_4.gz        # Very large file (200MB uncompressed)
└── README.md            # This file
```

## Setup and Installation

### Prerequisites
- Java 8 or higher
- Maven 3.6+ (recommended) OR Java compiler with manual dependency management
- At least 4GB RAM for large file testing
- Multi-core CPU (for meaningful multithreading comparison)

### Option 1: Maven Setup (Recommended)
```bash
# Clone or create the project directory
mkdir migz-benchmark && cd migz-benchmark

# Copy the provided files:
# - MiGzBenchmark.java
# - pom.xml  
# - setup_and_run.sh

# Make the script executable
chmod +x setup_and_run.sh

# Run the complete benchmark
./setup_and_run.sh
```

### Option 2: Manual Setup
```bash
# Download MiGZ library
curl -L "https://repo1.maven.org/maven2/com/linkedin/migz/migz/1.1.0/migz-1.1.0.jar" -o migz-1.1.0.jar

# Compile
javac -cp "migz-1.1.0.jar" MiGzBenchmark.java

# Create test files (see script for file generation)
mkdir resources
# ... create your test_1.gz, test_2.gz, etc. files

# Run benchmark
java -cp ".:migz-1.1.0.jar" -Xmx4g -Xms1g MiGzBenchmark
```

## How to Use

### Using Your Own Test Files
1. Place your `.gz` files in the `resources/` directory
2. Name them following the pattern: `test_1.gz`, `test_2.gz`, etc.
3. Run the benchmark - it will automatically detect and test all files

### Customizing the