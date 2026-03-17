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

    private static final String SECURITY_PROTOCOL_PLAINTEXT = "PLAINTEXT";
    private static final String SECURITY_PROTOCOL_SSL = "SSL";

    private static final String CRED_TRUSTSTORE_PASSWORD = "truststore_password";
    private static final String CRED_KEYSTORE_PASSWORD = "keystore_password";
    private static final String CRED_KEY_PASSWORD = "key_password";

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

        SslCredentials sslCredentials = null;
        if (appConfig.isSsl()) {
            sslCredentials = SslCredentials.loadFromCredentialsDirectory();
        }

        List<String> lines = Files.readAllLines(Path.of(appConfig.filePath), StandardCharsets.UTF_8)
                .stream()
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .collect(Collectors.toList());

        if (lines.isEmpty()) {
            throw new IllegalArgumentException("Input file is empty: " + appConfig.filePath);
        }

        Properties kafkaProps = buildKafkaProperties(appConfig, sslCredentials);
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

    private static Properties buildKafkaProperties(AppConfig appConfig, SslCredentials sslCredentials) {
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

        if (appConfig.isSsl()) {
            if (sslCredentials == null) {
                throw new IllegalStateException("SSL credentials were not loaded");
            }

            props.put("ssl.truststore.location", appConfig.truststoreLocation);
            props.put("ssl.truststore.password", sslCredentials.truststorePassword);
            props.put("ssl.keystore.location", appConfig.keystoreLocation);
            props.put("ssl.keystore.password", sslCredentials.keystorePassword);
            props.put("ssl.key.password", sslCredentials.keyPassword);
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
        String keystoreLocation;

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

            c.securityProtocol = p.getProperty("security.protocol", SECURITY_PROTOCOL_PLAINTEXT).trim();

            c.truststoreLocation = p.getProperty("ssl.truststore.location");
            c.keystoreLocation = p.getProperty("ssl.keystore.location");

            c.acks = p.getProperty("acks", "1").trim();
            c.lingerMs = Integer.parseInt(p.getProperty("linger.ms", "1").trim());
            c.batchSize = Integer.parseInt(p.getProperty("batch.size", "16384").trim());
            c.bufferMemory = Long.parseLong(p.getProperty("buffer.memory", "67108864").trim());

            return c;
        }

        boolean isSsl() {
            return SECURITY_PROTOCOL_SSL.equalsIgnoreCase(securityProtocol);
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

            if (!SECURITY_PROTOCOL_PLAINTEXT.equalsIgnoreCase(securityProtocol) &&
                !SECURITY_PROTOCOL_SSL.equalsIgnoreCase(securityProtocol)) {
                throw new IllegalArgumentException("security.protocol must be PLAINTEXT or SSL");
            }

            validateCompressionType(compressionType);

            if (isSsl()) {
                requireNonBlank(truststoreLocation, "ssl.truststore.location");
                requireNonBlank(keystoreLocation, "ssl.keystore.location");

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

    static class SslCredentials {
        final String truststorePassword;
        final String keystorePassword;
        final String keyPassword;

        SslCredentials(String truststorePassword, String keystorePassword, String keyPassword) {
            this.truststorePassword = truststorePassword;
            this.keystorePassword = keystorePassword;
            this.keyPassword = keyPassword;
        }

        static SslCredentials loadFromCredentialsDirectory() throws Exception {
            String credentialsDirectory = System.getenv("CREDENTIALS_DIRECTORY");

            if (credentialsDirectory == null || credentialsDirectory.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "security.protocol=SSL, but CREDENTIALS_DIRECTORY is not set. " +
                        "Make sure the service is started by systemd with LoadCredential= configured."
                );
            }

            Path credentialsDirPath = Path.of(credentialsDirectory);
            if (!Files.exists(credentialsDirPath) || !Files.isDirectory(credentialsDirPath)) {
                throw new IllegalArgumentException(
                        "security.protocol=SSL, but CREDENTIALS_DIRECTORY does not exist or is not a directory: " +
                        credentialsDirectory
                );
            }

            String truststorePassword = readRequiredCredential(credentialsDirPath, CRED_TRUSTSTORE_PASSWORD);
            String keystorePassword = readRequiredCredential(credentialsDirPath, CRED_KEYSTORE_PASSWORD);
            String keyPassword = readRequiredCredential(credentialsDirPath, CRED_KEY_PASSWORD);

            return new SslCredentials(truststorePassword, keystorePassword, keyPassword);
        }

        private static String readRequiredCredential(Path credentialsDir, String fileName) throws Exception {
            Path credentialFile = credentialsDir.resolve(fileName);

            if (!Files.exists(credentialFile)) {
                throw new IllegalArgumentException(
                        "Missing required SSL credential file: " + credentialFile
                );
            }

            if (!Files.isRegularFile(credentialFile)) {
                throw new IllegalArgumentException(
                        "SSL credential path is not a regular file: " + credentialFile
                );
            }

            String value = Files.readString(credentialFile, StandardCharsets.UTF_8).trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException(
                        "SSL credential file is empty: " + credentialFile
                );
            }

            return value;
        }
    }
}