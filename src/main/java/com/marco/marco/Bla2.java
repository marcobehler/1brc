package com.marco.marco;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class Bla2 {

    static long start;

    private static final byte SEMICOLON = (byte) ';';
    private static final byte NEWLINE = (byte) '\n';
    private static final byte CARRIAGE_RETURN = (byte) '\r';

    public static void main(String[] args) throws IOException {
        Path path = Path.of("./measurements.txt");

        start = System.nanoTime();
        new Bla2().process(path, Runtime.getRuntime().availableProcessors());

        long end = System.nanoTime();
        long time = (end - start) / 1_000_000;
        System.out.println("Took " + time + " ms");
    }

    private static final long MAX_MAPPING_SIZE = Integer.MAX_VALUE - 1024; // Leave some headroom

    public void process(Path file, int numThreads) throws IOException {
        long fileSize = Files.size(file);
        long chunkSize = fileSize / numThreads;


        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            List<FileSection> sections = splitInSections(numThreads, fileSize, chunkSize, channel);
            logTime();

        }

    }

    private static void logTime() {
        long end = System.nanoTime();
        long time = (end - start) / 1_000_000;
        System.out.println("Took " + time + " ms to split into chunks");
        start = System.nanoTime();
    }

    private List<FileSection> splitInSections(int numThreads, long fileSize, long chunkSize, FileChannel channel) throws IOException {
        List<FileSection> sections = new ArrayList<>();
        long currentPos = 0;

        for (int i = 0; i < numThreads; i++) {
            long startPos = currentPos;
            long endPos = (i == numThreads - 1) ? fileSize : currentPos + chunkSize;

            // Adjust end position to line boundary
            if (i < numThreads - 1) {
                endPos = findNextLineStart(channel, endPos, fileSize);
            }

            sections.add(new FileSection(startPos, endPos, i));
            currentPos = endPos;
        }
        return sections;
    }

    private void proess(Path file, int numThreads, List<FileSection> sections) throws IOException {

    }

    private long findNextLineStart(FileChannel channel, long approximatePos, long fileSize) throws IOException {
        if (approximatePos >= fileSize) return fileSize;

        // Use small buffer for boundary detection to avoid mapping issues
        ByteBuffer searchBuffer = ByteBuffer.allocate(8192);
        long searchPos = approximatePos;

        while (searchPos < fileSize) {
            searchBuffer.clear();
            int bytesRead = channel.read(searchBuffer, searchPos);
            if (bytesRead == -1) return fileSize;

            searchBuffer.flip();
            while (searchBuffer.hasRemaining()) {
                if (searchBuffer.get() == '\n') {
                    return searchPos + searchBuffer.position();
                }
            }
            searchPos += bytesRead;
        }
        return fileSize;
    }

    private void processLargeSection(FileChannel channel, Path file, FileSection section) throws IOException {
            long remainingBytes = section.endPos - section.startPos;
            long currentPos = section.startPos;
            StringBuilder lineBuilder = new StringBuilder();

            while (remainingBytes > 0) {
                // Map chunks that fit within Integer.MAX_VALUE
                long mappingSize = Math.min(remainingBytes, MAX_MAPPING_SIZE);

                // Adjust mapping size to end at line boundary (except for last chunk)
                if (remainingBytes > MAX_MAPPING_SIZE) {
                    mappingSize = adjustToLineBoundary(channel, currentPos, mappingSize);
                }

                MappedByteBuffer buffer = channel.map(
                        FileChannel.MapMode.READ_ONLY,
                        currentPos,
                        mappingSize
                );

                // Process this chunk
                processChunk(buffer, lineBuilder);

                currentPos += mappingSize;
                remainingBytes -= mappingSize;
            }
    }

    private long adjustToLineBoundary(FileChannel channel, long startPos, long desiredSize) throws IOException {
        // Find the last newline within the desired mapping size
        ByteBuffer searchBuffer = ByteBuffer.allocate(8192);
        long searchStart = startPos + desiredSize - searchBuffer.capacity();
        long lastNewlinePos = -1;

        searchBuffer.clear();
        int bytesRead = channel.read(searchBuffer, searchStart);
        searchBuffer.flip();

        while (searchBuffer.hasRemaining()) {
            if (searchBuffer.get() == '\n') {
                lastNewlinePos = searchStart + searchBuffer.position();
            }
        }

        return lastNewlinePos != -1 ? lastNewlinePos - startPos : desiredSize;
    }

    // Zero-copy version using CharsetDecoder for ultimate performance
    public static void parseZeroCopy(MappedByteBuffer buffer, RecordConsumer consumer) {
        int limit = buffer.limit();
        int position = 0;

        while (position < limit) {
            // Find key boundaries
            int keyStart = position;
            while (position < limit && buffer.get(position) != SEMICOLON) {
                position++;
            }

            if (position >= limit) break;

            int keyEnd = position;
            position++; // Skip semicolon

            // Find value boundaries
            int valueStart = position;
            while (position < limit) {
                byte b = buffer.get(position);
                if (b == NEWLINE || b == CARRIAGE_RETURN) break;
                position++;
            }

            int valueEnd = position;

            // Create slices and decode
            buffer.position(keyStart);
            buffer.limit(keyEnd);
            String key = StandardCharsets.UTF_8.decode(buffer).toString();

            buffer.position(valueStart);
            buffer.limit(valueEnd);
            String valueStr = StandardCharsets.UTF_8.decode(buffer).toString();

            double value = Double.parseDouble(valueStr);
            consumer.accept(key, value);

            // Reset limit and skip newlines
            buffer.limit(limit);
            position = valueEnd;
            while (position < limit &&
                    (buffer.get(position) == NEWLINE ||
                            buffer.get(position) == CARRIAGE_RETURN)) {
                position++;
            }
        }

        // Reset buffer state
        buffer.position(0);
        buffer.limit(limit);
    }

    private static byte[] expandBuffer(byte[] buffer) {
        byte[] newBuffer = new byte[buffer.length * 2];
        System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
        return newBuffer;
    }

    public interface RecordConsumer {
        void accept(String key, double value);
    }

    private void processChunk(MappedByteBuffer buffer, StringBuilder lineBuilder) {
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            char c = (char) (b & 0xFF);

            if (c == '\n') {
                String line = lineBuilder.toString().trim();
                if (!line.isEmpty()) {
                    processLine(line);
                }
                lineBuilder.setLength(0);
            } else if (c != '\r') {
                lineBuilder.append(c);
            }
        }
    }

    private void processLine(String line) {
        String[] parts = line.split(";", 2);
        if (parts.length == 2) {
            processKeyValue(parts[0], parts[1]);
        }
    }

    private void processKeyValue(String key, String value) {
        // Your processing logic
      //  System.out.println("Thread " + Thread.currentThread().getName() +
        //        ": " + key + " = " + value);
    }

    record FileSection(long startPos, long endPos, int threadId) {}
}
