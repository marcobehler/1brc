package com.marco.marco;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Bla {

    public static class FooRunnable implements Runnable {

        public final FileSection fileSection;
        public final Path path;
        private HashMap<String, Stats> cityToStats;

        public FooRunnable(FileSection section, Path path) {
            this.fileSection = section;
            this.path = path;
        }

        @Override
        public void run() {
            try {
                Map<String, List<Measurement>> cityToMeasurements = readLines(path, fileSection.startLine, fileSection.endLine);

                this.cityToStats = new HashMap<>();

                cityToMeasurements.forEach((key, value) -> {
                    Stats stats = calculateStats(value);
                    cityToStats.put(key, stats);
                });

            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public HashMap<String, Stats> getCityToStats() {
            return cityToStats;
        }
    }

    public static void main(String[] args) throws IOException {
        int threads = Runtime.getRuntime().availableProcessors();

        Path path = Path.of("./measurements.txt");
        long start = System.nanoTime();
        /*
         * for (int i = 0; i < 5; i++) {
         * 
         * }
         */
        long lineCount = Files.lines(path).count();

        System.out.println("lineCount: " + lineCount);

        long currentLine = 0;
        long linesPerThread = lineCount / threads;
        long remainingLines = lineCount % threads;
        FileSection[] fileSections = new FileSection[threads];
        for (int i = 0; i < threads; i++) {
            long startLine = currentLine;
            long endLine = startLine + linesPerThread;

            // Distribute remaining lines to first few threads
            if (i < remainingLines) {
                endLine++;
            }

            fileSections[i] = new FileSection(startLine, endLine);
            currentLine = endLine;
        }

        List<FooRunnable> fooRunnables = new ArrayList<>();
        Thread[] workers = new Thread[threads];
        for (int i = 0; i < workers.length; i++) {
            FooRunnable task = new FooRunnable(fileSections[i], path);
            fooRunnables.add(task);
            workers[i] = new Thread(task);
            workers[i].setName("Worker-Thread-" + i);
            workers[i].start();
        }

        // Wait for all threads to complete
        for (Thread thread : workers) {
            try {
                thread.join();
            }
            catch (InterruptedException e) {
                System.err.println("Thread interrupted: " + e.getMessage());
                Thread.currentThread().interrupt(); // Restore interrupt status
            }
        }

        Set<String> cities = new HashSet<>();
        fooRunnables.forEach(f -> {
            cities.addAll(f.getCityToStats().keySet());
        });

        cities.forEach(city -> {
            List<Stats> allStats = new ArrayList<>();
            fooRunnables.forEach(f -> {
                HashMap<String, Stats> cityToStats = f.getCityToStats();
                if (cityToStats.containsKey(city)) {
                    Stats stats = cityToStats.get(city);
                    allStats.add(stats);
                }
            });
            double globalMin = Math.round(allStats.stream().mapToDouble(r -> r.min).min().getAsDouble() * 10.0) / 10.0;;
            double globalMax = Math.round(allStats.stream().mapToDouble(r -> r.max).max().getAsDouble() * 10.0) / 10.0;;
            double globalMean = Math.round(allStats.stream().mapToDouble(r -> r.sum).sum() / allStats.size() * 10.0) / 10.0;;
            System.out.println(globalMin + "/" + globalMean + "/" + globalMax);
        });

        long end = System.nanoTime();
        long time = (end - start) / 1_000_000;
        System.out.println("Took " + time + " ms");

    }

    public static Map<String, List<Measurement>> readLines(Path path, long startLine, long endLine) throws IOException {
        Map<String, List<Measurement>> lines = new HashMap<>();

        // Each thread gets its own BufferedReader - no synchronization needed
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            // Skip to start line
            for (int i = 1; i < startLine; i++) {
                if (reader.readLine() == null)
                    return lines;
            }

            // Read the range
            for (long lineNum = startLine; lineNum <= endLine; lineNum++) {
                String line = reader.readLine();
                if (line == null || line.startsWith("#") || line.isBlank())
                    break;
                String[] split = line.split(";");
                Measurement e = new Measurement(split[0], Double.valueOf(split[1]));
                lines.putIfAbsent(e.city, new ArrayList<>());
                lines.get(e.city).add(e);
            }
        }
        return lines;
    }

    public static Stats calculateStats(List<Measurement> temperatures) {
        if (temperatures.isEmpty()) {
            throw new IllegalArgumentException("Array cannot be empty");
        }

        double min = temperatures.getFirst().measurement;
        double max = temperatures.getFirst().measurement;
        double sum = temperatures.getFirst().measurement;

        for (int i = 1; i < temperatures.size(); i++) {
            double temp = temperatures.get(i).measurement;
            if (temp < min)
                min = temp;
            if (temp > max)
                max = temp;
            sum += temp;
        }

        double mean = sum / temperatures.size();
        return new Stats(min, mean, max, sum);
    }

    public record Stats(double min, double mean, double max, double sum) {
    }

    public record Measurement(String city, Double measurement) {
    }

    public record FileSection(long startLine, long endLine) {
    }
}
