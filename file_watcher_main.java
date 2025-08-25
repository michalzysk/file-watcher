package com.company.filewatcher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableIntegration
@EnableAsync
public class FileWatcherApplication {

    public static void main(String[] args) {
        SpringApplication.run(FileWatcherApplication.class, args);
    }
}

// Configuration
package com.company.filewatcher.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "app")
@Data
public class AppConfig {
    
    private Smb smb = new Smb();
    private S3 s3 = new S3();
    private Trino trino = new Trino();
    private Processing processing = new Processing();
    private Pattern pattern = new Pattern();
    
    @Data
    public static class Smb {
        private String server = "nas.example.com";
        private String share = "data";
        private String username = "user";
        private String password = "password";
        private String watchPath = "/incoming";
    }
    
    @Data
    public static class S3 {
        private String bucket = "data-lake";
        private String region = "us-east-1";
        private String accessKeyId = "";
        private String secretAccessKey = "";
    }
    
    @Data
    public static class Trino {
        private String host = "trino.example.com";
        private int port = 443;
        private String user = "admin";
        private String catalog = "hive";
        private String schema = "default";
    }
    
    @Data
    public static class Processing {
        private int maxWorkers = 5;
        private int queueCapacity = 100;
        private long pollingInterval = 5000;
    }
    
    @Data
    public static class Pattern {
        private String filenameRegex = "(?<date>\\d{8})_(?<type>[A-Z]+)_(?<source>\\w+)\\.(?<ext>\\w+)";
        private String s3KeyTemplate = "data/{date}/{type}/{source}/{filename}";
    }
}

// SMB Client Service
package com.company.filewatcher.service;

import com.company.filewatcher.config.AppConfig;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.annotation.PreDestroy;
import javax.annotation.PostConstruct;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
@Slf4j
public class SmbService {
    
    @Autowired
    private AppConfig config;
    
    private SMBClient smbClient;
    private Connection connection;
    private Session session;
    private DiskShare share;
    
    @PostConstruct
    public void connect() {
        try {
            smbClient = new SMBClient();
            connection = smbClient.connect(config.getSmb().getServer());
            
            AuthenticationContext authContext = new AuthenticationContext(
                config.getSmb().getUsername(),
                config.getSmb().getPassword().toCharArray(),
                null
            );
            
            session = connection.authenticate(authContext);
            share = (DiskShare) session.connectShare(config.getSmb().getShare());
            
            log.info("SMB connection established to {}\\{}", 
                config.getSmb().getServer(), config.getSmb().getShare());
                
        } catch (Exception e) {
            log.error("Failed to connect to SMB", e);
            throw new RuntimeException("SMB connection failed", e);
        }
    }
    
    @PreDestroy
    public void disconnect() {
        try {
            if (share != null) share.close();
            if (session != null) session.close();
            if (connection != null) connection.close();
            if (smbClient != null) smbClient.close();
            log.info("SMB connection closed");
        } catch (Exception e) {
            log.warn("Error during SMB disconnect", e);
        }
    }
    
    public boolean isConnected() {
        return connection != null && connection.isConnected();
    }
    
    public Path downloadFile(String remotePath) throws IOException {
        String filename = Paths.get(remotePath).getFileName().toString();
        Path localPath = Paths.get(System.getProperty("java.io.tmpdir"), filename);
        
        try (File remoteFile = share.openFile(remotePath, null, null, null, null, null);
             InputStream inputStream = remoteFile.getInputStream();
             FileOutputStream outputStream = new FileOutputStream(localPath.toFile())) {
            
            byte[] buffer = new byte[64 * 1024]; // 64KB buffer
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            
            log.info("File downloaded: {} -> {}", remotePath, localPath);
            return localPath;
            
        } catch (Exception e) {
            log.error("Failed to download file: {}", remotePath, e);
            if (Files.exists(localPath)) {
                Files.delete(localPath);
            }
            throw new IOException("Download failed: " + remotePath, e);
        }
    }
}

// S3 Service
package com.company.filewatcher.service;

import com.company.filewatcher.config.AppConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.file.Path;

@Service
@Slf4j
public class S3Service {
    
    @Autowired
    private AppConfig config;
    
    private S3Client s3Client;
    
    @PostConstruct
    public void initialize() {
        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(
            config.getS3().getAccessKeyId(),
            config.getS3().getSecretAccessKey()
        );
        
        s3Client = S3Client.builder()
            .region(Region.of(config.getS3().getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
            .build();
            
        log.info("S3 client initialized for bucket: {}", config.getS3().getBucket());
    }
    
    @PreDestroy
    public void cleanup() {
        if (s3Client != null) {
            s3Client.close();
        }
    }
    
    public void uploadFile(Path localPath, String s3Key) {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                .bucket(config.getS3().getBucket())
                .key(s3Key)
                .build();
                
            s3Client.putObject(request, localPath);
            log.info("File uploaded to S3: {}", s3Key);
            
        } catch (Exception e) {
            log.error("Failed to upload file to S3: {}", s3Key, e);
            throw new RuntimeException("S3 upload failed: " + s3Key, e);
        }
    }
    
    public boolean fileExists(String s3Key) {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(config.getS3().getBucket())
                .key(s3Key)
                .build();
                
            s3Client.headObject(request);
            return true;
            
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            log.warn("Error checking S3 object existence: {}", s3Key, e);
            return false;
        }
    }
}

// Trino Service
package com.company.filewatcher.service;

import com.company.filewatcher.config.AppConfig;
import io.trino.jdbc.TrinoDriver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TrinoService {
    
    @Autowired
    private AppConfig config;
    
    private Connection connection;
    
    @PostConstruct
    public void initialize() {
        try {
            DriverManager.registerDriver(new TrinoDriver());
            
            String url = String.format("jdbc:trino://%s:%d/%s/%s",
                config.getTrino().getHost(),
                config.getTrino().getPort(),
                config.getTrino().getCatalog(),
                config.getTrino().getSchema()
            );
            
            Properties props = new Properties();
            props.setProperty("user", config.getTrino().getUser());
            props.setProperty("SSL", "true");
            
            connection = DriverManager.getConnection(url, props);
            log.info("Trino connection established to {}", url);
            
        } catch (Exception e) {
            log.error("Failed to connect to Trino", e);
            throw new RuntimeException("Trino connection failed", e);
        }
    }
    
    @PreDestroy
    public void cleanup() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception e) {
            log.warn("Error closing Trino connection", e);
        }
    }
    
    public void registerPartition(String tableName, Map<String, String> partitionSpec, String location) {
        String partitionClause = partitionSpec.entrySet().stream()
            .map(entry -> entry.getKey() + "='" + entry.getValue() + "'")
            .collect(Collectors.joining(", "));
            
        String query = String.format(
            "ALTER TABLE %s.%s.%s ADD IF NOT EXISTS PARTITION (%s) LOCATION 's3://%s/%s'",
            config.getTrino().getCatalog(),
            config.getTrino().getSchema(),
            tableName,
            partitionClause,
            config.getS3().getBucket(),
            location
        );
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query);
            log.info("Partition registered - Table: {}, Partition: {}", tableName, partitionSpec);
            
        } catch (Exception e) {
            log.error("Failed to register partition - Table: {}, Partition: {}", tableName, partitionSpec, e);
            throw new RuntimeException("Partition registration failed", e);
        }
    }
}

// File Processing Service
package com.company.filewatcher.service;

import com.company.filewatcher.config.AppConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class FileProcessingService {
    
    @Autowired
    private AppConfig config;
    
    @Autowired
    private SmbService smbService;
    
    @Autowired
    private S3Service s3Service;
    
    @Autowired
    private TrinoService trinoService;
    
    private final Counter filesProcessedSuccessCounter;
    private final Counter filesProcessedSkippedCounter;
    private final Counter filesProcessedDuplicateCounter;
    private final Counter filesProcessedErrorCounter;
    private final Timer processingTimer;
    private final Pattern filenamePattern;
    
    public FileProcessingService(MeterRegistry meterRegistry, AppConfig config) {
        this.filesProcessedSuccessCounter = Counter.builder("files.processed.total")
            .description("Total files processed successfully")
            .tag("status", "success")
            .register(meterRegistry);
            
        this.filesProcessedSkippedCounter = Counter.builder("files.processed.total")
            .description("Total files skipped")
            .tag("status", "skipped")
            .register(meterRegistry);
            
        this.filesProcessedDuplicateCounter = Counter.builder("files.processed.total")
            .description("Total duplicate files")
            .tag("status", "duplicate")
            .register(meterRegistry);
            
        this.filesProcessedErrorCounter = Counter.builder("files.processed.total")
            .description("Total files with errors")
            .tag("status", "error")
            .register(meterRegistry);
            
        this.processingTimer = Timer.builder("file.processing.duration")
            .description("Time spent processing files")
            .register(meterRegistry);
            
        this.filenamePattern = Pattern.compile(config.getPattern().getFilenameRegex());
    }
    
    public void processFile(String filePath) {
        Timer.Sample sample = Timer.start();
        String filename = Paths.get(filePath).getFileName().toString();
        
        try {
            log.info("Processing file: {}", filename);
            
            // Parse filename
            Map<String, String> metadata = parseFilename(filename);
            if (metadata.isEmpty()) {
                log.warn("Filename doesn't match pattern: {}", filename);
                filesProcessedCounter.increment(Tags.of("status", "skipped"));
                return;
            }
            
            // Generate S3 key
            String s3Key = generateS3Key(filename, metadata);
            
            // Check if already processed
            if (s3Service.fileExists(s3Key)) {
                log.info("File already exists in S3, skipping: {}", s3Key);
                filesProcessedCounter.increment(Tags.of("status", "duplicate"));
                return;
            }
            
            // Download from SMB
            Path localPath = smbService.downloadFile(filePath);
            
            try {
                // Upload to S3
                s3Service.uploadFile(localPath, s3Key);
                
                // Register Trino partition
                registerTrinoPartition(metadata, s3Key);
                
                log.info("File processed successfully: {} -> {}", filename, s3Key);
                filesProcessedCounter.increment(Tags.of("status", "success"));
                
            } finally {
                // Cleanup local file
                try {
                    Files.deleteIfExists(localPath);
                } catch (Exception e) {
                    log.warn("Failed to delete temporary file: {}", localPath, e);
                }
            }
            
        } catch (Exception e) {
            log.error("Error processing file: {}", filename, e);
            filesProcessedCounter.increment(Tags.of("status", "error"));
            throw new RuntimeException("File processing failed: " + filename, e);
            
        } finally {
            sample.stop(processingTimer);
        }
    }
    
    private Map<String, String> parseFilename(String filename) {
        Matcher matcher = filenamePattern.matcher(filename);
        if (matcher.matches()) {
            Map<String, String> metadata = new HashMap<>();
            try {
                metadata.put("date", matcher.group("date"));
                metadata.put("type", matcher.group("type"));
                metadata.put("source", matcher.group("source"));
                metadata.put("ext", matcher.group("ext"));
                return metadata;
            } catch (IllegalArgumentException e) {
                log.warn("Failed to extract groups from filename: {}", filename, e);
            }
        }
        return Map.of(); // Java 9+ Map.of() for empty map
    }
    
    private String generateS3Key(String filename, Map<String, String> metadata) {
        String template = config.getPattern().getS3KeyTemplate();
        String result = template.replace("{filename}", filename);
        
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            result = result.replace(STR."{{\{entry.getKey()}}}", entry.getValue()); // Java 21 String Templates
        }
        
        return result;
    }
    
    private void registerTrinoPartition(Map<String, String> metadata, String s3Key) {
        try {
            String tableName = STR."\{metadata.get("type").toLowerCase()}_data"; // Java 21 String Templates
            
            Map<String, String> partitionSpec = Map.of(
                "date", metadata.get("date"),
                "source", metadata.get("source")
            );
            
            // Remove filename from S3 key to get location path
            String location = s3Key.substring(0, s3Key.lastIndexOf('/'));
            
            trinoService.registerPartition(tableName, partitionSpec, location);
            
        } catch (Exception e) {
            log.warn("Failed to register Trino partition for S3 key: {}", s3Key, e);
            // Don't fail the whole process if partition registration fails
        }
    }
}

// Spring Integration Configuration
package com.company.filewatcher.config;

import com.company.filewatcher.service.FileProcessingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.RegexPatternFileListFilter;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import java.io.File;
import java.time.Duration;

@Configuration
@EnableIntegration
@Slf4j
public class IntegrationConfig {
    
    @Autowired
    private AppConfig appConfig;
    
    @Autowired
    private FileProcessingService fileProcessingService;
    
    @Bean
    public MessageChannel fileInputChannel() {
        return new QueueChannel(appConfig.getProcessing().getQueueCapacity());
    }
    
    @Bean
    public MessageChannel fileProcessingChannel() {
        return new QueueChannel(appConfig.getProcessing().getQueueCapacity());
    }
    
    @Bean
    public MessageChannel errorChannel() {
        return new QueueChannel(100);
    }
    
    @Bean
    public ThreadPoolTaskExecutor fileProcessingTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(appConfig.getProcessing().getMaxWorkers());
        executor.setQueueCapacity(appConfig.getProcessing().getQueueCapacity());
        executor.setThreadNamePrefix("file-processor-");
        executor.setRejectedExecutionHandler((r, executor1) -> {
            log.warn("Task rejected, queue is full. Consider increasing capacity or processing speed.");
            throw new MessagingException("Processing queue is full");
        });
        executor.initialize();
        return executor;
    }
    
    @Bean
    public MessageSource<File> fileReadingMessageSource() {
        FileReadingMessageSource source = new FileReadingMessageSource();
        source.setDirectory(new File(appConfig.getSmb().getWatchPath()));
        source.setAutoCreateDirectory(false);
        source.setUseWatchService(true); // Use WatchService for better performance in 6.5.1
        source.setWatchEvents(FileReadingMessageSource.WatchEventType.CREATE, 
                             FileReadingMessageSource.WatchEventType.MODIFY);
        
        // File filters
        CompositeFileListFilter<File> filters = new CompositeFileListFilter<>();
        filters.addFilter(new AcceptOnceFileListFilter<>());
        filters.addFilter(new RegexPatternFileListFilter(appConfig.getPattern().getFilenameRegex()));
        source.setFilter(filters);
        
        return source;
    }
    
    @Bean
    public IntegrationFlow fileWatchingFlow() {
        return IntegrationFlows
            .from(fileReadingMessageSource(),
                  c -> c.poller(Pollers.fixedDelay(Duration.ofMillis(appConfig.getProcessing().getPollingInterval()))
                                      .maxMessagesPerPoll(1)
                                      .taskExecutor(fileProcessingTaskExecutor())
                                      .errorChannel(errorChannel())))
            .enrichHeaders(h -> h.header("processedAt", System.currentTimeMillis()))
            .log(LoggingHandler.Level.INFO, "file.watcher", 
                 m -> "File detected: " + m.getPayload() + " at " + m.getHeaders().get("processedAt"))
            .channel(fileProcessingChannel())
            .handle(fileProcessingMessageHandler())
            .get();
    }
    
    @Bean
    public IntegrationFlow errorHandlingFlow() {
        return IntegrationFlows
            .from(errorChannel())
            .log(LoggingHandler.Level.ERROR, "file.watcher.error")
            .handle(message -> {
                Exception exception = (Exception) message.getPayload();
                log.error("Error processing file", exception);
                // Could implement dead letter queue, alerts, etc.
            })
            .get();
    }
    
    @Bean
    @ServiceActivator(inputChannel = "fileProcessingChannel")
    public MessageHandler fileProcessingMessageHandler() {
        return message -> {
            File file = (File) message.getPayload();
            try {
                fileProcessingService.processFile(file.getAbsolutePath());
            } catch (Exception e) {
                log.error("Failed to process file: {}", file.getAbsolutePath(), e);
                // Send to error channel for proper error handling
                errorChannel().send(MessageBuilder.withPayload(e)
                    .setHeader("originalFile", file.getAbsolutePath())
                    .setHeader("errorTime", System.currentTimeMillis())
                    .build());
                throw e; // Re-throw to trigger retry if configured
            }
        };
    }
    
    @Bean
    public IntegrationFlow monitoringFlow() {
        return IntegrationFlows
            .fromSupplier(() -> "monitoring-tick", 
                         c -> c.poller(Pollers.fixedRate(Duration.ofMinutes(1))))
            .handle(message -> {
                // Emit custom metrics about queue sizes, processing rates, etc.
                log.debug("Queue size - Processing: {}, Error: {}", 
                    ((QueueChannel) fileProcessingChannel()).getQueueSize(),
                    ((QueueChannel) errorChannel()).getQueueSize());
            })
            .get();
    }
}

// Health Check Controller
package com.company.filewatcher.controller;

import com.company.filewatcher.service.SmbService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuator.health.Health;
import org.springframework.boot.actuator.health.HealthIndicator;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@RestController
public class HealthController implements HealthIndicator {
    
    @Autowired
    private SmbService smbService;
    
    @Override
    public Health health() {
        boolean smbConnected = smbService.isConnected();
        
        if (smbConnected) {
            return Health.up()
                .withDetail("smb", "connected")
                .withDetail("timestamp", LocalDateTime.now())
                .build();
        } else {
            return Health.down()
                .withDetail("smb", "disconnected")
                .withDetail("timestamp", LocalDateTime.now())
                .build();
        }
    }
    
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> status = new HashMap<>();
        status.put("status", "UP");
        status.put("timestamp", LocalDateTime.now());
        status.put("smb_connected", smbService.isConnected());
        
        return ResponseEntity.ok(status);
    }
    
    @GetMapping("/ready")
    public ResponseEntity<Map<String, Object>> readinessCheck() {
        Map<String, Object> status = new HashMap<>();
        boolean ready = smbService.isConnected();
        
        status.put("status", ready ? "ready" : "not ready");
        status.put("timestamp", LocalDateTime.now());
        status.put("smb_connected", ready);
        
        return ResponseEntity.status(ready ? 200 : 503).body(status);
    }
}