#!/bin/bash

# MiGZ Benchmark Setup and Execution Script
# This script creates test files and runs the benchmark

set -e

echo "=== MiGZ Benchmark Setup ==="

# Create resources directory
mkdir -p resources

# Function to create test files of different sizes
create_test_file() {
    local size_mb=$1
    local filename=$2
    local temp_file="temp_${filename%.gz}"
    
    echo "Creating test file: $filename (${size_mb}MB uncompressed)"
    
    # Create a file with random text data that compresses well
    # Using a mix of repeated patterns and random data
    {
        # Add some structure that compresses well
        for i in $(seq 1 $(($size_mb * 100))); do
            echo "This is line $i with some repeated text content that should compress reasonably well."
            echo "Random data: $(date +%s%N | sha256sum | cut -c1-32)"
            echo "Pattern: ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            echo ""
        done
        
        # Add some actual random data to make it more realistic
        head -c $(($size_mb * 1024 * 200)) /dev/urandom | base64
        
    } > "$temp_file"
    
    # Compress with standard gzip
    gzip -c "$temp_file" > "resources/$filename"
    
    # Clean up temp file
    rm "$temp_file"
    
    local compressed_size=$(du -h "resources/$filename" | cut -f1)
    echo "  Compressed size: $compressed_size"
}

# Check if test files already exist
if [ "$(ls -A resources/*.gz 2>/dev/null | wc -l)" -gt 0 ]; then
    echo "Test files already exist in resources/ directory:"
    ls -lh resources/*.gz
    echo ""
    echo "Do you want to recreate them? (y/N)"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        echo "Using existing test files."
        skip_creation=true
    fi
fi

# Create test files if needed
if [ "$skip_creation" != true ]; then
    echo "Creating test files with different sizes..."
    
    # Small file (10MB uncompressed)
    create_test_file 10 "test_1.gz"
    
    # Medium file (50MB uncompressed)  
    create_test_file 50 "test_2.gz"
    
    # Large file (100MB uncompressed)
    create_test_file 100 "test_3.gz"
    
    # Very large file (200MB uncompressed)
    create_test_file 200 "test_4.gz"
    
    # Extra large file (500MB uncompressed) - uncomment if you want to test with very large files
    # create_test_file 500 "test_5.gz"
    
    echo ""
    echo "Test files created:"
    ls -lh resources/*.gz
fi

echo ""
echo "=== Compiling and Running Benchmark ==="

# Check if Maven is available
if command -v mvn &> /dev/null; then
    echo "Using Maven to compile and run..."
    
    # Compile the project
    echo "Compiling..."
    mvn clean compile
    
    # Run the benchmark
    echo "Starting benchmark..."
    mvn exec:java -Dexec.mainClass="MiGzBenchmark"
    
elif command -v javac &> /dev/null && command -v java &> /dev/null; then
    echo "Using javac/java directly..."
    
    # Download MiGZ JAR if not present
    if [ ! -f "migz-1.1.0.jar" ]; then
        echo "Downloading MiGZ library..."
        curl -L "https://repo1.maven.org/maven2/com/linkedin/migz/migz/1.1.0/migz-1.1.0.jar" -o migz-1.1.0.jar
    fi
    
    # Compile
    echo "Compiling Java files..."
    javac -cp "migz-1.1.0.jar" MiGzBenchmark.java
    
    # Run
    echo "Starting benchmark..."
    java -cp ".:migz-1.1.0.jar" -Xmx4g -Xms1g MiGzBenchmark
    
else
    echo "Error: Neither Maven nor Java compiler found!"
    echo "Please install Java JDK and Maven, or run the Java compilation manually."
    exit 1
fi

echo ""
echo "=== Benchmark Complete ==="
echo "Check the output above for performance comparison results."
