package loader;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class KafkaLoadGenerator {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: java loader.KafkaLoadGenerator <config.properties>");
            System.exit(1);
        }

        Path configPath = Path.of(args[0]);
        if (!Files.exists(configPath)) {
            throw new IllegalArgumentException("Config file does not exist: " + configPath);
        }

        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(configPath)) {
            properties.load(in);
        }

        AppConfig appConfig = AppConfig.fromProperties(properties);
        appConfig.validate();

        List<String> lines = Files.readAllLines(Path.of(appConfig.filePath), StandardCharsets.UTF_8)
                .stream()
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .collect(Collectors.toList());

        if (lines.isEmpty()) {
            throw new IllegalArgumentException("Input file is empty: " + appConfig.filePath);
        }

        Properties kafkaProps = buildKafkaProperties(appConfig);
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

        int totalThreads = appConfig.topics.size() * appConfig.threadsPerTopic;
        ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        ScheduledExecutorService statsExecutor = Executors.newSingleThreadScheduledExecutor();

        AtomicBoolean running = new AtomicBoolean(true);

        AtomicLong sentOk = new AtomicLong(0);
        AtomicLong sentFailed = new AtomicLong(0);

        AtomicLong lastOk = new AtomicLong(0);
        AtomicLong lastFailed = new AtomicLong(0);

        long statsPeriodSeconds = 5;
        long expectedTotalEps = (long) appConfig.topics.size() * appConfig.epsPerTopic;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown requested...");

            running.set(false);

            statsExecutor.shutdownNow();
            executor.shutdownNow();

            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.out.println("Workers did not stop within 10 seconds.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            try {
                producer.flush();
            } catch (Exception e) {
                System.err.println("Producer flush failed during shutdown: " + e.getMessage());
            }

            try {
                producer.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                System.err.println("Producer close failed during shutdown: " + e.getMessage());
            }

            System.out.println("Shutdown complete.");
        }));

        printStartupInfo(appConfig, lines.size(), totalThreads, expectedTotalEps);

        statsExecutor.scheduleAtFixedRate(() -> {
            long okTotal = sentOk.get();
            long failedTotal = sentFailed.get();

            long okDelta = okTotal - lastOk.getAndSet(okTotal);
            long failedDelta = failedTotal - lastFailed.getAndSet(failedTotal);

            double okEps = okDelta / (double) statsPeriodSeconds;
            double failedEps = failedDelta / (double) statsPeriodSeconds;

            System.out.println(
                    "Stats: expected_total_eps=" + expectedTotalEps +
                    ", actual_ok_eps=" + String.format("%.2f", okEps) +
                    ", actual_failed_eps=" + String.format("%.2f", failedEps) +
                    ", sent_ok_total=" + okTotal +
                    ", sent_failed_total=" + failedTotal
            );
        }, statsPeriodSeconds, statsPeriodSeconds, TimeUnit.SECONDS);

        for (String topic : appConfig.topics) {
            AtomicInteger cursor = new AtomicInteger(0);
            double epsPerThread = appConfig.epsPerTopic / (double) appConfig.threadsPerTopic;

            System.out.println(
                    "Starting workers for topic=" + topic +
                    ", threads=" + appConfig.threadsPerTopic +
                    ", eps/thread≈" + epsPerThread
            );

            for (int i = 0; i < appConfig.threadsPerTopic; i++) {
                executor.submit(() -> {
                    RateLimiter limiter = RateLimiter.create(epsPerThread);

                    Callback callback = (metadata, exception) -> {
                        if (exception == null) {
                            sentOk.incrementAndGet();
                        } else {
                            sentFailed.incrementAndGet();
                            if (running.get()) {
                                System.err.println("Send failed for topic=" + topic + ": " + exception.getMessage());
                            }
                        }
                    };

                    while (running.get() && !Thread.currentThread().isInterrupted()) {
                        try {
                            boolean acquired = limiter.tryAcquire(1, 200, TimeUnit.MILLISECONDS);
                            if (!acquired) {
                                continue;
                            }

                            if (!running.get() || Thread.currentThread().isInterrupted()) {
                                break;
                            }

                            int idx = Math.floorMod(cursor.getAndIncrement(), lines.size());
                            String line = lines.get(idx);

                            ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
                            producer.send(record, callback);

                        } catch (Exception e) {
                            if (!running.get() || Thread.currentThread().isInterrupted()) {
                                break;
                            }

                            sentFailed.incrementAndGet();
                            System.err.println("Worker error for topic=" + topic + ": " + e.getMessage());
                        }
                    }
                });
            }
        }
    }

    private static Properties buildKafkaProperties(AppConfig appConfig) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, appConfig.acks);
        props.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(appConfig.lingerMs));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(appConfig.batchSize));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(appConfig.bufferMemory));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, appConfig.compressionType);

        props.put("security.protocol", appConfig.securityProtocol);

        if ("SSL".equalsIgnoreCase(appConfig.securityProtocol)) {
            props.put("ssl.truststore.location", appConfig.truststoreLocation);
            props.put("ssl.truststore.password", appConfig.truststorePassword);
            props.put("ssl.keystore.location", appConfig.keystoreLocation);
            props.put("ssl.keystore.password", appConfig.keystorePassword);
            props.put("ssl.key.password", appConfig.keyPassword);
        }

        return props;
    }

    private static void printStartupInfo(AppConfig config, int linesCount, int totalThreads, long expectedTotalEps) {
        System.out.println("Config loaded successfully");
        System.out.println("Bootstrap servers: " + config.bootstrapServers);
        System.out.println("Topics: " + config.topics);
        System.out.println("File path: " + config.filePath);
        System.out.println("Security protocol: " + config.securityProtocol);
        System.out.println("Compression: " + config.compressionType);
        System.out.println("Threads per topic: " + config.threadsPerTopic);
        System.out.println("EPS per topic: " + config.epsPerTopic);
        System.out.println("Expected total EPS: " + expectedTotalEps);
        System.out.println("Total threads: " + totalThreads);
        System.out.println("Loaded lines: " + linesCount);
    }

    static class AppConfig {
        String bootstrapServers;
        List<String> topics;
        String filePath;

        int threadsPerTopic;
        int epsPerTopic;
        String compressionType;

        String securityProtocol;

        String truststoreLocation;
        String truststorePassword;
        String keystoreLocation;
        String keystorePassword;
        String keyPassword;

        String acks;
        int lingerMs;
        int batchSize;
        long bufferMemory;

        static AppConfig fromProperties(Properties p) {
            AppConfig c = new AppConfig();

            c.bootstrapServers = required(p, "bootstrap.servers");
            c.topics = Arrays.stream(required(p, "topics").split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .distinct()
                    .collect(Collectors.toList());

            c.filePath = required(p, "file.path");

            c.threadsPerTopic = Integer.parseInt(required(p, "threads.per.topic"));
            c.epsPerTopic = Integer.parseInt(required(p, "eps.per.topic"));
            c.compressionType = p.getProperty("compression.type", "none").trim();

            c.securityProtocol = p.getProperty("security.protocol", "PLAINTEXT").trim();

            c.truststoreLocation = p.getProperty("ssl.truststore.location");
            c.truststorePassword = p.getProperty("ssl.truststore.password");
            c.keystoreLocation = p.getProperty("ssl.keystore.location");
            c.keystorePassword = p.getProperty("ssl.keystore.password");
            c.keyPassword = p.getProperty("ssl.key.password", c.keystorePassword);

            c.acks = p.getProperty("acks", "1").trim();
            c.lingerMs = Integer.parseInt(p.getProperty("linger.ms", "1").trim());
            c.batchSize = Integer.parseInt(p.getProperty("batch.size", "16384").trim());
            c.bufferMemory = Long.parseLong(p.getProperty("buffer.memory", "67108864").trim());

            return c;
        }

        void validate() {
            if (topics.isEmpty()) {
                throw new IllegalArgumentException("Property topics must contain at least one topic");
            }

            if (threadsPerTopic <= 0) {
                throw new IllegalArgumentException("threads.per.topic must be > 0");
            }

            if (epsPerTopic <= 0) {
                throw new IllegalArgumentException("eps.per.topic must be > 0");
            }

            if (!Files.exists(Path.of(filePath))) {
                throw new IllegalArgumentException("Input file does not exist: " + filePath);
            }

            if (!"PLAINTEXT".equalsIgnoreCase(securityProtocol) &&
                !"SSL".equalsIgnoreCase(securityProtocol)) {
                throw new IllegalArgumentException("security.protocol must be PLAINTEXT or SSL");
            }

            validateCompressionType(compressionType);

            if ("SSL".equalsIgnoreCase(securityProtocol)) {
                requireNonBlank(truststoreLocation, "ssl.truststore.location");
                requireNonBlank(truststorePassword, "ssl.truststore.password");
                requireNonBlank(keystoreLocation, "ssl.keystore.location");
                requireNonBlank(keystorePassword, "ssl.keystore.password");
                requireNonBlank(keyPassword, "ssl.key.password");

                if (!Files.exists(Path.of(truststoreLocation))) {
                    throw new IllegalArgumentException("Truststore does not exist: " + truststoreLocation);
                }

                if (!Files.exists(Path.of(keystoreLocation))) {
                    throw new IllegalArgumentException("Keystore does not exist: " + keystoreLocation);
                }
            }
        }

        private static void validateCompressionType(String compressionType) {
            List<String> allowed = List.of("none", "gzip", "snappy", "lz4", "zstd");
            if (!allowed.contains(compressionType.toLowerCase())) {
                throw new IllegalArgumentException(
                        "compression.type must be one of: " + allowed + ", got: " + compressionType
                );
            }
        }

        private static String required(Properties p, String key) {
            String value = p.getProperty(key);
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException("Missing required property: " + key);
            }
            return value.trim();
        }

        private static void requireNonBlank(String value, String key) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException("Missing required property: " + key);
            }
        }
    }
}